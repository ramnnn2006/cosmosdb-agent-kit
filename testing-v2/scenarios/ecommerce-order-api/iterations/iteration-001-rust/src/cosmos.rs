use azure_core::http::StatusCode;
use azure_data_cosmos::{
    models::{ContainerProperties, IndexingPolicy, PartitionKeyDefinition},
    CosmosAccountEndpoint, CosmosAccountReference, CosmosClient, CosmosClientBuilder,
    ItemOptions, PartitionKey, Query,
};
use futures::TryStreamExt;
use serde_json::Value;
use std::sync::Arc;

use crate::models::Order;

/// Wrapper around the official Azure Cosmos DB Rust SDK.
#[derive(Clone)]
pub struct CosmosDb {
    client: CosmosClient,
    database: String,
    container: String,
}

impl CosmosDb {
    pub async fn new(endpoint: &str, key: &str, database: &str, container: &str) -> Result<Self, String> {
        let endpoint: CosmosAccountEndpoint = endpoint
            .parse()
            .map_err(|e| format!("Invalid endpoint: {}", e))?;

        let account = CosmosAccountReference::with_master_key(
            endpoint,
            azure_core::credentials::Secret::from(key.to_string()),
        );

        let client = CosmosClientBuilder::new()
            .with_allow_emulator_invalid_certificates(true)
            .build(account)
            .await
            .map_err(|e| format!("Failed to build CosmosClient: {}", e))?;

        Ok(Self {
            client,
            database: database.to_string(),
            container: container.to_string(),
        })
    }

    /// Initialize database and container (create if not exists).
    pub async fn init(&self) -> Result<(), String> {
        // Create database (ignore 409 Conflict = already exists)
        match self.client.create_database(&self.database, None).await {
            Ok(_) => {}
            Err(e) if e.http_status() == Some(StatusCode::Conflict) => {}
            Err(e) => return Err(format!("create database failed: {}", e)),
        }

        // Create container with /customerId partition key and composite indexes
        let db_client = self.client.database_client(&self.database);

        // Build indexing policy with composite indexes via JSON deserialization
        // (CompositeIndex/CompositeIndexProperty are #[non_exhaustive])
        let indexing_policy: IndexingPolicy = serde_json::from_value(serde_json::json!({
            "automatic": true,
            "indexingMode": "consistent",
            "includedPaths": [{"path": "/*"}],
            "excludedPaths": [{"path": "/_etag/?"}],
            "compositeIndexes": [
                [
                    {"path": "/status", "order": "ascending"},
                    {"path": "/createdAt", "order": "descending"}
                ],
                [
                    {"path": "/customerId", "order": "ascending"},
                    {"path": "/createdAt", "order": "descending"}
                ]
            ]
        })).expect("valid indexing policy JSON");

        let properties = ContainerProperties::new(
            self.container.clone(),
            PartitionKeyDefinition::new(vec!["/customerId".to_string()]),
        )
        .with_indexing_policy(indexing_policy);

        match db_client.create_container(properties, None).await {
            Ok(_) => {}
            Err(e) if e.http_status() == Some(StatusCode::Conflict) => {}
            Err(e) => return Err(format!("create container failed: {}", e)),
        }

        Ok(())
    }

    /// Get a container client.
    async fn container_client(
        &self,
    ) -> azure_data_cosmos::clients::ContainerClient {
        let db_client = self.client.database_client(&self.database);
        db_client.container_client(&self.container).await
    }

    /// Create a document in the container.
    pub async fn create_document(&self, order: &Order) -> Result<(), String> {
        let container = self.container_client().await;
        let pk = PartitionKey::from(order.customer_id.clone());
        let item = serde_json::to_value(order).map_err(|e| format!("serialize: {}", e))?;

        container
            .create_item(pk, item, None)
            .await
            .map_err(|e| format!("create item failed: {}", e))?;

        Ok(())
    }

    /// Replace (update) a document.
    pub async fn replace_document(&self, order: &Order) -> Result<(), String> {
        let container = self.container_client().await;
        let pk = PartitionKey::from(order.customer_id.clone());
        let item = serde_json::to_value(order).map_err(|e| format!("serialize: {}", e))?;

        let options = ItemOptions::default().with_content_response_on_write_enabled(false);

        container
            .replace_item(pk, &order.id, item, Some(options))
            .await
            .map_err(|e| format!("replace item failed: {}", e))?;

        Ok(())
    }

    /// Delete a document by id and partition key.
    pub async fn delete_document(&self, doc_id: &str, partition_key: &str) -> Result<bool, String> {
        let container = self.container_client().await;
        let pk_owned = partition_key.to_string();

        match container.delete_item(pk_owned, doc_id, None).await {
            Ok(_) => Ok(true),
            Err(e) if e.http_status() == Some(StatusCode::NotFound) => Ok(false),
            Err(e) => Err(format!("delete item failed: {}", e)),
        }
    }

    /// Execute a SQL query against the container.
    /// Uses cross-partition query when no partition key is provided.
    pub async fn query_documents(
        &self,
        query: Query,
        partition_key: Option<&str>,
    ) -> Result<Vec<Order>, String> {
        let container = self.container_client().await;

        let pk = match partition_key {
            Some(pk) => PartitionKey::from(pk.to_string()),
            None => PartitionKey::EMPTY,
        };

        let mut items = container
            .query_items::<Value>(query, pk, None)
            .map_err(|e| format!("query failed: {}", e))?;

        let mut orders: Vec<Order> = Vec::new();
        while let Some(item) = items
            .try_next()
            .await
            .map_err(|e| format!("query stream error: {}", e))?
        {
            if let Ok(order) = serde_json::from_value::<Order>(item) {
                orders.push(order);
            }
        }

        Ok(orders)
    }

    /// Execute a SQL query and return raw JSON values (for aggregates).
    pub async fn query_raw(
        &self,
        query: Query,
        partition_key: Option<&str>,
    ) -> Result<Vec<Value>, String> {
        let container = self.container_client().await;

        let pk = match partition_key {
            Some(pk) => PartitionKey::from(pk.to_string()),
            None => PartitionKey::EMPTY,
        };

        let mut items = container
            .query_items::<Value>(query, pk, None)
            .map_err(|e| format!("query failed: {}", e))?;

        let mut results: Vec<Value> = Vec::new();
        while let Some(item) = items
            .try_next()
            .await
            .map_err(|e| format!("query stream error: {}", e))?
        {
            results.push(item);
        }

        Ok(results)
    }
}

pub type SharedCosmos = Arc<CosmosDb>;

