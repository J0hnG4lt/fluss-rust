/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use parking_lot::RwLock;
use std::sync::Arc;
use std::sync::LazyLock;

use crate::integration::fluss_cluster::FlussTestingCluster;
#[cfg(test)]
use test_env_helpers::*;

static SHARED_FLUSS_CLUSTER: LazyLock<Arc<RwLock<Option<FlussTestingCluster>>>> =
    LazyLock::new(|| Arc::new(RwLock::new(None)));

#[cfg(test)]
#[before_all]
#[after_all]
mod pk_table_cdc_test {
    use super::SHARED_FLUSS_CLUSTER;
    use crate::integration::fluss_cluster::FlussTestingCluster;
    use crate::integration::utils::{create_table, get_cluster, start_cluster, stop_cluster};
    use fluss::client::EARLIEST_OFFSET;
    use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
    use fluss::record::ChangeType;
    use fluss::row::{GenericRow, InternalRow};
    use std::sync::Arc;
    use std::time::Duration;

    fn before_all() {
        start_cluster("pk_cdc_test", SHARED_FLUSS_CLUSTER.clone());
    }

    fn get_fluss_cluster() -> Arc<FlussTestingCluster> {
        get_cluster(&SHARED_FLUSS_CLUSTER)
    }

    fn after_all() {
        stop_cluster(SHARED_FLUSS_CLUSTER.clone());
    }

    /// Test that PK tables with ARROW log format can be scanned and return
    /// proper ChangeType values (Insert, not hardcoded AppendOnly).
    #[tokio::test]
    async fn pk_table_arrow_format_scan() {
        let cluster = get_fluss_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().await.expect("get admin");

        let table_path = TablePath::new("fluss", "test_pk_cdc_arrow");

        let schema = Schema::builder()
            .column("user_id", DataTypes::int())
            .column("name", DataTypes::string())
            .column("score", DataTypes::int())
            .primary_key(vec!["user_id"])
            .build()
            .expect("build schema");

        let table_descriptor = TableDescriptor::builder()
            .schema(schema)
            .distributed_by(Some(1), vec!["user_id".to_string()])
            .property("table.log.format", "ARROW")
            .build()
            .expect("build table descriptor");

        create_table(&admin, &table_path, &table_descriptor).await;

        // Wait for table to be fully initialized
        tokio::time::sleep(Duration::from_secs(2)).await;

        let table = connection
            .get_table(&table_path)
            .await
            .expect("get table");

        // Upsert 3 rows (should produce Insert change types)
        let upsert = table.new_upsert().expect("new upsert");
        let writer = upsert.create_writer().expect("create writer");

        for (id, name, score) in [(1, "Alice", 100), (2, "Bob", 200), (3, "Charlie", 150)] {
            let mut row = GenericRow::new(3);
            row.set_field(0, id);
            row.set_field(1, name);
            row.set_field(2, score);
            writer.upsert(&row).expect("upsert row");
        }
        writer.flush().await.expect("flush");

        // Scan the table — this should NOT fail with "doesn't support scan"
        let log_scanner = table
            .new_scan()
            .create_log_scanner()
            .expect("create_log_scanner should succeed for PK table with ARROW format");

        log_scanner
            .subscribe(0, EARLIEST_OFFSET)
            .await
            .expect("subscribe");

        // Poll for records
        let mut records = Vec::new();
        let start = std::time::Instant::now();
        while records.len() < 3 && start.elapsed() < Duration::from_secs(15) {
            let scan_records = log_scanner
                .poll(Duration::from_millis(500))
                .await
                .expect("poll");
            for rec in scan_records {
                let ct = *rec.change_type();
                let user_id = rec.row().get_int(0);
                let name = rec.row().get_string(1).to_string();
                let score = rec.row().get_int(2);
                records.push((ct, user_id, name, score));
            }
        }

        assert_eq!(records.len(), 3, "Expected 3 records from PK table scan");

        // All initial inserts should have Insert change type (NOT AppendOnly)
        for (ct, user_id, _name, _score) in &records {
            assert_eq!(
                *ct,
                ChangeType::Insert,
                "Expected Insert change type for user_id={user_id}, got {ct}"
            );
        }

        // Verify data
        records.sort_by_key(|r| r.1);
        assert_eq!(records[0].1, 1);
        assert_eq!(records[0].2, "Alice");
        assert_eq!(records[1].1, 2);
        assert_eq!(records[1].2, "Bob");
        assert_eq!(records[2].1, 3);
        assert_eq!(records[2].2, "Charlie");
    }

    /// Test that upserting a row that already exists produces UpdateBefore/UpdateAfter
    /// change types in the CDC log.
    #[tokio::test]
    async fn pk_table_update_change_types() {
        let cluster = get_fluss_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().await.expect("get admin");

        let table_path = TablePath::new("fluss", "test_pk_cdc_updates");

        let schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("value", DataTypes::string())
            .primary_key(vec!["id"])
            .build()
            .expect("build schema");

        let table_descriptor = TableDescriptor::builder()
            .schema(schema)
            .distributed_by(Some(1), vec!["id".to_string()])
            .property("table.log.format", "ARROW")
            .build()
            .expect("build table descriptor");

        create_table(&admin, &table_path, &table_descriptor).await;
        tokio::time::sleep(Duration::from_secs(2)).await;

        let table = connection
            .get_table(&table_path)
            .await
            .expect("get table");

        let upsert = table.new_upsert().expect("new upsert");
        let writer = upsert.create_writer().expect("create writer");

        // Insert a row
        let mut row1 = GenericRow::new(2);
        row1.set_field(0, 1i32);
        row1.set_field(1, "original");
        writer.upsert(&row1).expect("upsert");
        writer.flush().await.expect("flush");

        // Update the same row (same PK)
        let mut row2 = GenericRow::new(2);
        row2.set_field(0, 1i32);
        row2.set_field(1, "updated");
        writer.upsert(&row2).expect("upsert update");
        writer.flush().await.expect("flush update");

        // Scan to see all CDC events
        let log_scanner = table
            .new_scan()
            .create_log_scanner()
            .expect("create log scanner");

        log_scanner
            .subscribe(0, EARLIEST_OFFSET)
            .await
            .expect("subscribe");

        let mut all_records = Vec::new();
        let start = std::time::Instant::now();
        // We expect at least: 1 Insert + 1 UpdateBefore + 1 UpdateAfter = 3
        while all_records.len() < 3 && start.elapsed() < Duration::from_secs(15) {
            let scan_records = log_scanner
                .poll(Duration::from_millis(500))
                .await
                .expect("poll");
            for rec in scan_records {
                all_records.push((*rec.change_type(), rec.row().get_string(1).to_string()));
            }
        }

        // Verify we got at least one Insert and the update pair
        let insert_count = all_records
            .iter()
            .filter(|(ct, _)| *ct == ChangeType::Insert)
            .count();
        let update_before_count = all_records
            .iter()
            .filter(|(ct, _)| *ct == ChangeType::UpdateBefore)
            .count();
        let update_after_count = all_records
            .iter()
            .filter(|(ct, _)| *ct == ChangeType::UpdateAfter)
            .count();

        assert!(
            insert_count >= 1,
            "Expected at least 1 Insert, got {insert_count}. All records: {all_records:?}"
        );
        assert!(
            update_before_count >= 1,
            "Expected at least 1 UpdateBefore, got {update_before_count}. All records: {all_records:?}"
        );
        assert!(
            update_after_count >= 1,
            "Expected at least 1 UpdateAfter, got {update_after_count}. All records: {all_records:?}"
        );

        // Verify the update values
        let update_after = all_records
            .iter()
            .find(|(ct, _)| *ct == ChangeType::UpdateAfter)
            .expect("should have UpdateAfter");
        assert_eq!(update_after.1, "updated");
    }

    /// Test that RecordBatchLogScanner also works for PK tables.
    #[tokio::test]
    async fn pk_table_batch_scanner() {
        let cluster = get_fluss_cluster();
        let connection = cluster.get_fluss_connection().await;
        let admin = connection.get_admin().await.expect("get admin");

        let table_path = TablePath::new("fluss", "test_pk_cdc_batch");

        let schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("name", DataTypes::string())
            .primary_key(vec!["id"])
            .build()
            .expect("build schema");

        let table_descriptor = TableDescriptor::builder()
            .schema(schema)
            .distributed_by(Some(1), vec!["id".to_string()])
            .property("table.log.format", "ARROW")
            .build()
            .expect("build table descriptor");

        create_table(&admin, &table_path, &table_descriptor).await;
        tokio::time::sleep(Duration::from_secs(2)).await;

        let table = connection
            .get_table(&table_path)
            .await
            .expect("get table");

        let upsert = table.new_upsert().expect("new upsert");
        let writer = upsert.create_writer().expect("create writer");

        for (id, name) in [(10, "X"), (20, "Y")] {
            let mut row = GenericRow::new(2);
            row.set_field(0, id);
            row.set_field(1, name);
            writer.upsert(&row).expect("upsert");
        }
        writer.flush().await.expect("flush");

        // Use RecordBatchLogScanner — should succeed (no PK guard)
        let batch_scanner = table
            .new_scan()
            .create_record_batch_log_scanner()
            .expect("create_record_batch_log_scanner should succeed for PK ARROW table");

        batch_scanner.subscribe(0, 0).await.expect("subscribe");

        let mut total_rows = 0;
        let start = std::time::Instant::now();
        while total_rows < 2 && start.elapsed() < Duration::from_secs(15) {
            let batches = batch_scanner
                .poll(Duration::from_millis(500))
                .await
                .expect("poll");
            for b in &batches {
                total_rows += b.num_records();
            }
        }

        assert_eq!(total_rows, 2, "Expected 2 records from batch scanner");
    }
}
