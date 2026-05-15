# News Intelligence Pipeline

A end-to-end data pipeline that ingests news articles, transforms them through multiple layers, and generates vector embeddings for semantic search and analysis.

## Architecture

```
NewsAPI
    ↓
Bronze (Raw Articles)
    ↓
Silver (DLT - Cleaned & Deduplicated)
    ↓
Gold (Embeddings - Azure OpenAI)
    ↓
Monitoring (Pipeline Runs Metadata)
```

## Pipeline Layers

### Bronze Layer
- **Source**: NewsAPI (`https://newsapi.org/v2/everything`)
- **Storage**: Unity Catalog Volume (`/Volumes/news_pipeline/bronze/raw_json_files/`)
- **Ingestion**: Auto Loader (Structured Streaming)
- **Table**: `news_pipeline.bronze.raw_articles`
- **Records**: Raw articles in JSONL format

### Silver Layer
- **Type**: Delta Live Tables (DLT) Pipeline
- **Transformations**:
  - Parse timestamps (`publishedAt`)
  - Clean HTML tags from content
  - Remove `[+N chars]` truncation markers
  - Flatten nested `source` struct
  - Extract author country code from parentheses
  - Trim whitespace
- **Deduplication**: SCD Type 2 (slowly changing dimensions)
- **Key**: `url` (unique identifier)
- **Sequence**: `ingest_time` (for tracking changes)
- **Table**: `news_pipeline.silver.silver_articles`
- **Expectations** (DLT):
  - `valid_url`: url is not null
  - `valid_title`: title is not null

### Gold Layer
- **Type**: Batch processing with Pandas UDFs
- **Transformation**: Text embedding generation
- **Text**: Concatenation of `title` + `description`
- **Model**: Azure OpenAI `text-embedding-3-small` (1536-dim vectors)
- **Deduplication**: Left anti-join to avoid re-embedding existing URLs
- **Table**: `news_pipeline.gold.gold_articles_embeddings`
- **Output Columns**:
  - All silver columns + `__START_AT`, `__END_AT` (SCD Type 2)
  - `embedding` (float array, 1536 dimensions)
  - `ingested_at` (timestamp)

### Monitoring Layer
- **Table**: `news_pipeline.monitoring.pipeline_runs`
- **Tracked Metrics**:
  - `run_id` (unique identifier)
  - `pipeline_name`, `topic`
  - `start_time`, `end_time`, `duration_spent`
  - `status` (running/succeeded/failed)
  - `bronze_records`, `silver_records`, `gold_records`
  - `error_message`

## Notebooks

### 1. `01_auto_loader_ingest.ipynb` (Bronze)
- Fetches articles from NewsAPI with retry logic (exponential backoff)
- Saves JSON responses to volume
- Streams JSON files into Bronze table via Auto Loader
- Logs pipeline start to monitoring table
- **Widgets**: `topic`, `page_size`, `language`
- **Duration**: ~10-20 seconds

### 2. `02_dlt_pipeline.ipynb` (Silver)
- DLT pipeline with cleaning transformations
- Creates `cleaned_articles` view with expectations
- Applies CDC (change data capture) with SCD Type 2 to `silver_articles`
- Automatically triggered by Workflow
- **Duration**: ~20-30 seconds

### 3. `03_embedding_pipeline.ipynb` (Gold)
- Reads active silver records (where `__END_AT IS NULL`)
- Batches text through Azure OpenAI embedding API
- Handles null/empty texts gracefully
- Incremental append on repeat runs (avoids re-embedding)
- Updates monitoring table with final counts and status
- **Duration**: ~15-25 seconds (depends on batch size)

## Workflow

**Name**: `news-intelligence-pipeline-daily`

**Tasks**:
1. `bronze_ingestion` → runs `01_auto_loader_ingest`
2. `silver_transformation` → triggers DLT pipeline `news-intelligence-silver`
3. `gold_embedding` → runs `03_embedding_pipeline`
4. `log_pipeline_failure` → runs on failure (conditional)

**Schedule**: Daily at 06:00 AM UTC+05:30 (India/Kolkata)

**Parameters**:
```json
{
  "topic": "india",
  "page_size": 100,
  "language": "en"
}
```

## Setup Instructions

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- Azure storage account (ADLS Gen2)
- NewsAPI key (https://newsapi.org/)
- Azure OpenAI API key (text-embedding-3-small model)

### Environment Variables
Set these as Databricks workspace secrets:

```
NEWS_API_KEY = <your-newsapi-key>
NEWS_API_BASE_URL = https://newsapi.org/v2/everything
OPENAI_API_KEY = <your-azure-openai-key>
OPENAI_ENDPOINT = <your-azure-openai-embedding-endpoint>
```

### Catalog & Schemas
```sql
CREATE CATALOG news_pipeline
MANAGED LOCATION 'abfss://<your-container>@<your-storage-account>.dfs.core.windows.net/';

CREATE SCHEMA news_pipeline.bronze;
CREATE SCHEMA news_pipeline.silver;
CREATE SCHEMA news_pipeline.gold;
CREATE SCHEMA news_pipeline.monitoring;

CREATE EXTERNAL VOLUME news_pipeline.bronze.raw_json_files
LOCATION 'abfss://<your-container>@<your-storage-account>.dfs.core.windows.net/bronze/raw/';
```

### Monitoring Table
```sql
CREATE TABLE news_pipeline.monitoring.pipeline_runs (
    run_id STRING,
    pipeline_name STRING,
    topic STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_spent DOUBLE,
    status STRING,
    bronze_records INT,
    silver_records INT,
    gold_records INT,
    error_message STRING
);
```

### Upload Notebooks
1. Create folder structure: `/news-intelligence-pipeline/{bronze,silver,gold}/`
2. Upload notebooks to respective folders:
   - `bronze/01_auto_loader_ingest`
   - `silver/02_dlt_pipeline`
   - `gold/03_embedding_pipeline`

### Create DLT Pipeline
1. Go to **Jobs & Pipelines** → **Create** → **Delta Live Tables**
2. **Pipeline name**: `news-intelligence-silver`
3. **Source notebook**: `silver/02_dlt_pipeline`
4. **Catalog**: `news_pipeline`
5. **Schema**: `silver`

### Create Workflow
1. **New Job** → **Workflow**
2. **Add task**:
   - Name: `bronze_ingestion`
   - Type: `Notebook`
   - Path: `bronze/01_auto_loader_ingest`
   - Cluster: `Sanch's cluster` (or your cluster)
   - Parameters: `{"topic": "india", "page_size": "100", "language": "en"}`

3. **Add task**:
   - Name: `silver_transformation`
   - Type: `Pipeline`
   - Pipeline ID: (select `news-intelligence-silver`)
   - Depends on: `bronze_ingestion`

4. **Add task**:
   - Name: `gold_embedding`
   - Type: `Notebook`
   - Path: `gold/03_embedding_pipeline`
   - Cluster: `Sanch's cluster`
   - Depends on: `silver_transformation`

5. **Add task**:
   - Name: `log_pipeline_failure`
   - Type: `Notebook`
   - Path: `log_failure`
   - Trigger condition: `if at least one failed`

6. **Schedule**: Daily at 06:00 AM UTC+05:30

## Monitoring

### Check Pipeline Status
```sql
SELECT * FROM news_pipeline.monitoring.pipeline_runs
ORDER BY start_time DESC
LIMIT 10;
```

### View Silver Expectations
```sql
SELECT * FROM news_pipeline.silver.silver_articles
LIMIT 5;
```

### Check Embeddings
```sql
SELECT url, title, SIZE(embedding) as embedding_dim, ingested_at
FROM news_pipeline.gold.gold_articles_embeddings
LIMIT 5;
```

### Find Duplicates (SCD Type 2)
```sql
SELECT url, COUNT(*) as versions
FROM news_pipeline.silver.silver_articles
GROUP BY url
HAVING COUNT(*) > 1
ORDER BY versions DESC;
```

## Performance Metrics

**Typical Run** (79 articles):
- Bronze ingestion: 10-15s
- Silver DLT transform: 20-30s
- Gold embedding: 15-25s
- **Total**: 60-70 seconds

**Bottlenecks**:
- Azure OpenAI API latency (batch processing)
- DLT checkpoint writes
- Auto Loader schema detection (first run only)

## Troubleshooting

### Bronze Task Fails
- **Check**: NewsAPI key, connectivity, rate limits
- **Logs**: Run output in Databricks workflow
- **Fix**: Retry manually or wait for next scheduled run

### Silver DLT Fails
- **Check**: Bronze table exists and has valid data
- **Logs**: DLT event log in pipeline UI
- **Fix**: Re-run DLT pipeline manually via "Run Now"

### Gold Embedding Fails
- **Check**: Azure OpenAI credentials, endpoint, API limits
- **Logs**: Notebook output in workflow run
- **Fix**: Check error message in monitoring table

### Records Not Appearing
- **Bronze**: Check volume path and Auto Loader checkpoints
- **Silver**: Verify DLT pipeline ran successfully
- **Gold**: Check for null embeddings (API failures)

## Future Enhancements

- [ ] Data quality monitoring table
- [ ] Semantic search UI
- [ ] Article clustering by topic
- [ ] Sentiment analysis integration
- [ ] Vector DB sink (Pinecone/Weaviate)
- [ ] Real-time streaming ingestion
- [ ] Multi-language support with translation
- [ ] Duplicate detection via embedding similarity

## Repository Structure

```
news-intelligence-pipeline/
├── README.md
├── bronze/
│   └── 01_auto_loader_ingest.ipynb
├── silver/
│   └── 02_dlt_pipeline.ipynb
├── gold/
│   └── 03_embedding_pipeline.ipynb
└── log_failure.ipynb
```

## License

Internal use only
