/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.prelert.action.DeleteJobAction;
import org.elasticsearch.xpack.prelert.job.CategorizerState;
import org.elasticsearch.xpack.prelert.job.DataCounts;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.ModelSizeStats;
import org.elasticsearch.xpack.prelert.job.ModelSnapshot;
import org.elasticsearch.xpack.prelert.job.ModelState;
import org.elasticsearch.xpack.prelert.job.audit.Auditor;
import org.elasticsearch.xpack.prelert.job.persistence.BucketsQueryBuilder.BucketsQuery;
import org.elasticsearch.xpack.prelert.job.persistence.InfluencersQueryBuilder.InfluencersQuery;
import org.elasticsearch.xpack.prelert.job.quantiles.Quantiles;
import org.elasticsearch.xpack.prelert.job.results.AnomalyRecord;
import org.elasticsearch.xpack.prelert.job.results.Bucket;
import org.elasticsearch.xpack.prelert.job.results.CategoryDefinition;
import org.elasticsearch.xpack.prelert.job.results.Influencer;
import org.elasticsearch.xpack.prelert.job.results.ModelDebugOutput;
import org.elasticsearch.xpack.prelert.job.results.ReservedFieldNames;
import org.elasticsearch.xpack.prelert.job.results.Result;
import org.elasticsearch.xpack.prelert.job.usage.Usage;
import org.elasticsearch.xpack.prelert.lists.ListDocument;
import org.elasticsearch.xpack.prelert.utils.ExceptionsHelper;
import org.elasticsearch.xpack.prelert.utils.time.TimeUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class ElasticsearchJobProvider implements JobProvider
{
    private static final Logger LOGGER = Loggers.getLogger(ElasticsearchJobProvider.class);

    /**
     * The index to store total usage/metering information
     */
    public static final String PRELERT_USAGE_INDEX = "prelert-usage";

    /**
     * Where to store the prelert info in Elasticsearch - must match what's
     * expected by kibana/engineAPI/app/directives/prelertLogUsage.js
     */
    private static final String PRELERT_INFO_INDEX = "prelert-int";

    private static final String SETTING_TRANSLOG_DURABILITY = "index.translog.durability";
    private static final String ASYNC = "async";
    private static final String SETTING_MAPPER_DYNAMIC = "index.mapper.dynamic";
    private static final String SETTING_DEFAULT_ANALYZER_TYPE = "index.analysis.analyzer.default.type";
    private static final String KEYWORD = "keyword";

    private static final List<String> SECONDARY_SORT = Arrays.asList(
            AnomalyRecord.ANOMALY_SCORE.getPreferredName(),
            AnomalyRecord.OVER_FIELD_VALUE.getPreferredName(),
            AnomalyRecord.PARTITION_FIELD_VALUE.getPreferredName(),
            AnomalyRecord.BY_FIELD_VALUE.getPreferredName(),
            AnomalyRecord.FIELD_NAME.getPreferredName(),
            AnomalyRecord.FUNCTION.getPreferredName()
            );

    private static final int RECORDS_SIZE_PARAM = 500;


    private final Client client;
    private final int numberOfReplicas;
    private final ParseFieldMatcher parseFieldMatcher;

    public ElasticsearchJobProvider(Client client, int numberOfReplicas, ParseFieldMatcher parseFieldMatcher) {
        this.parseFieldMatcher = parseFieldMatcher;
        this.client = Objects.requireNonNull(client);
        this.numberOfReplicas = numberOfReplicas;
    }

    public void initialize() {
        LOGGER.info("Connecting to Elasticsearch cluster '" + client.settings().get("cluster.name")
                + "'");

        // This call was added because if we try to connect to Elasticsearch
        // while it's doing the recovery operations it does at startup then we
        // can get weird effects like indexes being reported as not existing
        // when they do.  See EL16-182 in Jira.
        LOGGER.trace("ES API CALL: wait for yellow status on whole cluster");
        ClusterHealthResponse response = client.admin().cluster()
                .prepareHealth()
                .setWaitForYellowStatus()
                .execute().actionGet();

        // The wait call above can time out.
        // Throw an error if in cluster health is red
        if (response.getStatus() == ClusterHealthStatus.RED) {
            String msg = "Waited for the Elasticsearch status to be YELLOW but is RED after wait timeout";
            LOGGER.error(msg);
            throw new IllegalStateException(msg);
        }

        LOGGER.info("Elasticsearch cluster '" + client.settings().get("cluster.name")
                + "' now ready to use");


        createUsageMeteringIndex();
    }

    /**
     * If the {@value ElasticsearchJobProvider#PRELERT_USAGE_INDEX} index does
     * not exist then create it here with the usage document mapping.
     */
    private void createUsageMeteringIndex() {
        try {
            LOGGER.trace("ES API CALL: index exists? {}", PRELERT_USAGE_INDEX);
            boolean indexExists = client.admin().indices()
                    .exists(new IndicesExistsRequest(PRELERT_USAGE_INDEX))
                    .get().isExists();

            if (indexExists == false) {
                LOGGER.info("Creating the internal '" + PRELERT_USAGE_INDEX + "' index");

                XContentBuilder usageMapping = ElasticsearchMappings.usageMapping();

                LOGGER.trace("ES API CALL: create index {}", PRELERT_USAGE_INDEX);
                client.admin().indices().prepareCreate(PRELERT_USAGE_INDEX)
                .setSettings(prelertIndexSettings())
                .addMapping(Usage.TYPE, usageMapping)
                .get();
                LOGGER.trace("ES API CALL: wait for yellow status {}", PRELERT_USAGE_INDEX);
                client.admin().cluster().prepareHealth(PRELERT_USAGE_INDEX).setWaitForYellowStatus().execute().actionGet();
            }
        } catch (InterruptedException | ExecutionException | IOException e) {
            LOGGER.warn("Error checking the usage metering index", e);
        } catch (ResourceAlreadyExistsException e) {
            LOGGER.debug("Usage metering index already exists", e);
        }
    }

    /**
     * Build the Elasticsearch index settings that we want to apply to Prelert
     * indexes.  It's better to do this in code rather than in elasticsearch.yml
     * because then the settings can be applied regardless of whether we're
     * using our own Elasticsearch to store results or a customer's pre-existing
     * Elasticsearch.
     * @return An Elasticsearch builder initialised with the desired settings
     * for Prelert indexes.
     */
    private Settings.Builder prelertIndexSettings()
    {
        return Settings.builder()
                // Our indexes are small and one shard puts the
                // least possible burden on Elasticsearch
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)
                // Sacrifice durability for performance: in the event of power
                // failure we can lose the last 5 seconds of changes, but it's
                // much faster
                .put(SETTING_TRANSLOG_DURABILITY, ASYNC)
                // We need to allow fields not mentioned in the mappings to
                // pick up default mappings and be used in queries
                .put(SETTING_MAPPER_DYNAMIC, true)
                // By default "analyzed" fields won't be tokenised
                .put(SETTING_DEFAULT_ANALYZER_TYPE, KEYWORD);
    }

    /**
     * Create the Elasticsearch index and the mappings
     */
    @Override
    public void createJobRelatedIndices(Job job, ActionListener<Boolean> listener) {
        Collection<String> termFields = (job.getAnalysisConfig() != null) ? job.getAnalysisConfig().termFields() : Collections.emptyList();
        try {
            XContentBuilder resultsMapping = ElasticsearchMappings.resultsMapping(termFields);
            XContentBuilder categorizerStateMapping = ElasticsearchMappings.categorizerStateMapping();
            XContentBuilder categoryDefinitionMapping = ElasticsearchMappings.categoryDefinitionMapping();
            XContentBuilder quantilesMapping = ElasticsearchMappings.quantilesMapping();
            XContentBuilder modelStateMapping = ElasticsearchMappings.modelStateMapping();
            XContentBuilder modelSnapshotMapping = ElasticsearchMappings.modelSnapshotMapping();
            XContentBuilder modelSizeStatsMapping = ElasticsearchMappings.modelSizeStatsMapping();
            XContentBuilder modelDebugMapping = ElasticsearchMappings.modelDebugOutputMapping(termFields);
            XContentBuilder processingTimeMapping = ElasticsearchMappings.processingTimeMapping();
            XContentBuilder partitionScoreMapping = ElasticsearchMappings.bucketPartitionMaxNormalizedScores();
            XContentBuilder dataCountsMapping = ElasticsearchMappings.dataCountsMapping();

            String jobId = job.getId();
            LOGGER.trace("ES API CALL: create index {}", job.getId());
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(JobResultsPersister.getJobIndexName(jobId));
            createIndexRequest.settings(prelertIndexSettings());
            createIndexRequest.mapping(Result.TYPE.getPreferredName(), resultsMapping);
            createIndexRequest.mapping(CategorizerState.TYPE, categorizerStateMapping);
            createIndexRequest.mapping(CategoryDefinition.TYPE.getPreferredName(), categoryDefinitionMapping);
            createIndexRequest.mapping(Quantiles.TYPE.getPreferredName(), quantilesMapping);
            createIndexRequest.mapping(ModelState.TYPE, modelStateMapping);
            createIndexRequest.mapping(ModelSnapshot.TYPE.getPreferredName(), modelSnapshotMapping);
            createIndexRequest.mapping(ModelSizeStats.TYPE.getPreferredName(), modelSizeStatsMapping);
            createIndexRequest.mapping(ModelDebugOutput.TYPE.getPreferredName(), modelDebugMapping);
            createIndexRequest.mapping(ReservedFieldNames.BUCKET_PROCESSING_TIME_TYPE, processingTimeMapping);
            createIndexRequest.mapping(ReservedFieldNames.PARTITION_NORMALIZED_PROB_TYPE, partitionScoreMapping);
            createIndexRequest.mapping(DataCounts.TYPE.getPreferredName(), dataCountsMapping);

            client.admin().indices().create(createIndexRequest, new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(CreateIndexResponse createIndexResponse) {
                    listener.onResponse(true);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public void deleteJobRelatedIndices(String jobId, ActionListener<DeleteJobAction.Response> listener) {
        String indexName = JobResultsPersister.getJobIndexName(jobId);
        LOGGER.trace("ES API CALL: delete index {}", indexName);

        try {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
            client.admin().indices().delete(deleteIndexRequest, new ActionListener<DeleteIndexResponse>() {
                @Override
                public void onResponse(DeleteIndexResponse deleteIndexResponse) {
                    listener.onResponse(new DeleteJobAction.Response(deleteIndexResponse.isAcknowledged()));
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public DataCounts dataCounts(String jobId) {
        String indexName = JobResultsPersister.getJobIndexName(jobId);

        try {
            GetResponse response = client.prepareGet(indexName, DataCounts.TYPE.getPreferredName(),
                    jobId + DataCounts.DOCUMENT_SUFFIX).get();
            if (response.isExists() == false) {
                return new DataCounts(jobId);
            } else {
                BytesReference source = response.getSourceAsBytesRef();
                XContentParser parser;
                try {
                    parser = XContentFactory.xContent(source).createParser(source);
                    return DataCounts.PARSER.apply(parser, () -> parseFieldMatcher);
                } catch (IOException e) {
                    throw new ElasticsearchParseException("failed to parser bucket", e);
                }
            }

        } catch (IndexNotFoundException e) {
            throw ExceptionsHelper.missingJobException(jobId);
        }
    }

    @Override
    public QueryPage<Bucket> buckets(String jobId, BucketsQuery query)
            throws ResourceNotFoundException {
        QueryBuilder fb = new ResultsFilterBuilder()
                .timeRange(ElasticsearchMappings.ES_TIMESTAMP, query.getEpochStart(), query.getEpochEnd())
                .score(Bucket.ANOMALY_SCORE.getPreferredName(), query.getAnomalyScoreFilter())
                .score(Bucket.MAX_NORMALIZED_PROBABILITY.getPreferredName(), query.getNormalizedProbability())
                .interim(Bucket.IS_INTERIM.getPreferredName(), query.isIncludeInterim())
                .build();

        SortBuilder<?> sortBuilder = new FieldSortBuilder(esSortField(query.getSortField()))
                .order(query.isSortDescending() ? SortOrder.DESC : SortOrder.ASC);

        QueryPage<Bucket> buckets = buckets(jobId, query.isIncludeInterim(), query.getFrom(), query.getSize(), fb, sortBuilder);

        if (Strings.isNullOrEmpty(query.getPartitionValue())) {
            for (Bucket b : buckets.results()) {
                if (query.isExpand() && b.getRecordCount() > 0) {
                    expandBucket(jobId, query.isIncludeInterim(), b);
                }
            }
        } else {
            List<ScoreTimestamp> scores =
                    partitionScores(jobId, query.getEpochStart(), query.getEpochEnd(), query.getPartitionValue());

            mergePartitionScoresIntoBucket(scores, buckets.results());

            for (Bucket b : buckets.results()) {
                if (query.isExpand() && b.getRecordCount() > 0) {
                    this.expandBucketForPartitionValue(jobId, query.isIncludeInterim(), b, query.getPartitionValue());
                }

                b.setAnomalyScore(b.partitionAnomalyScore(query.getPartitionValue()));
            }
        }

        return buckets;
    }

    void mergePartitionScoresIntoBucket(List<ScoreTimestamp> scores, List<Bucket> buckets) {
        Iterator<ScoreTimestamp> itr = scores.iterator();
        ScoreTimestamp score = itr.hasNext() ? itr.next() : null;
        for (Bucket b : buckets) {
            if (score == null) {
                b.setMaxNormalizedProbability(0.0);
            } else {
                if (score.timestamp.equals(b.getTimestamp())) {
                    b.setMaxNormalizedProbability(score.score);
                    score = itr.hasNext() ? itr.next() : null;
                } else {
                    b.setMaxNormalizedProbability(0.0);
                }
            }
        }
    }

    private QueryPage<Bucket> buckets(String jobId, boolean includeInterim, int from, int size,
                                      QueryBuilder fb, SortBuilder<?> sb) throws ResourceNotFoundException {

        QueryBuilder boolQuery = new BoolQueryBuilder()
                .filter(fb)
                .filter(new TermsQueryBuilder(Result.RESULT_TYPE.getPreferredName(), Bucket.RESULT_TYPE_VALUE));

        SearchResponse searchResponse;
        try {
            String indexName = JobResultsPersister.getJobIndexName(jobId);
            LOGGER.trace("ES API CALL: search all of result type {}  from index {} with filter from {} size {}",
                    Bucket.RESULT_TYPE_VALUE, indexName, from, size);

            searchResponse = client.prepareSearch(indexName)
                    .setTypes(Result.TYPE.getPreferredName())
                    .addSort(sb)
                    .setQuery(new ConstantScoreQueryBuilder(boolQuery))
                    .setFrom(from).setSize(size)
                    .get();
        } catch (IndexNotFoundException e) {
            throw ExceptionsHelper.missingJobException(jobId);
        }

        List<Bucket> results = new ArrayList<>();

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            BytesReference source = hit.getSourceRef();
            XContentParser parser;
            try {
                parser = XContentFactory.xContent(source).createParser(source);
            } catch (IOException e) {
                throw new ElasticsearchParseException("failed to parser bucket", e);
            }
            Bucket bucket = Bucket.PARSER.apply(parser, () -> parseFieldMatcher);
            bucket.setId(hit.getId());

            if (includeInterim || bucket.isInterim() == false) {
                results.add(bucket);
            }
        }

        return new QueryPage<>(results, searchResponse.getHits().getTotalHits(), Bucket.RESULTS_FIELD);
    }


    @Override
    public QueryPage<Bucket> bucket(String jobId, BucketQueryBuilder.BucketQuery query) throws ResourceNotFoundException {
        String indexName = JobResultsPersister.getJobIndexName(jobId);
        SearchHits hits;
        try {
            LOGGER.trace("ES API CALL: get Bucket with timestamp {} from index {}", query.getTimestamp(), indexName);
            QueryBuilder matchQuery = QueryBuilders.matchQuery(ElasticsearchMappings.ES_TIMESTAMP, query.getTimestamp());

            QueryBuilder boolQuery = new BoolQueryBuilder()
                    .filter(matchQuery)
                    .filter(new TermsQueryBuilder(Result.RESULT_TYPE.getPreferredName(), Bucket.RESULT_TYPE_VALUE));

            SearchResponse searchResponse = client.prepareSearch(indexName)
                    .setTypes(Result.TYPE.getPreferredName())
                    .setQuery(boolQuery)
                    .addSort(SortBuilders.fieldSort(ElasticsearchMappings.ES_DOC))
                    .get();
            hits = searchResponse.getHits();
        } catch (IndexNotFoundException e) {
            throw ExceptionsHelper.missingJobException(jobId);
        }

        if (hits.getTotalHits() == 0) {
            throw QueryPage.emptyQueryPage(Bucket.RESULTS_FIELD);
        } else if (hits.getTotalHits() > 1L) {
            LOGGER.error("Found more than one bucket with timestamp [" + query.getTimestamp() + "]" +
                    " from index " + indexName);
        }

        SearchHit hit = hits.getAt(0);
        BytesReference source = hit.getSourceRef();
        XContentParser parser;
        try {
            parser = XContentFactory.xContent(source).createParser(source);
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to parser bucket", e);
        }
        Bucket bucket = Bucket.PARSER.apply(parser, () -> parseFieldMatcher);
        bucket.setId(hit.getId());

        // don't return interim buckets if not requested
        if (bucket.isInterim() && query.isIncludeInterim() == false) {
            throw QueryPage.emptyQueryPage(Bucket.RESULTS_FIELD);
        }

        if (Strings.isNullOrEmpty(query.getPartitionValue())) {
            if (query.isExpand() && bucket.getRecordCount() > 0) {
                expandBucket(jobId, query.isIncludeInterim(), bucket);
            }
        } else {
            List<ScoreTimestamp> scores =
                    partitionScores(jobId,
                            query.getTimestamp(), query.getTimestamp() + 1,
                            query.getPartitionValue());

            bucket.setMaxNormalizedProbability(scores.isEmpty() == false ?
                    scores.get(0).score : 0.0d);
            if (query.isExpand() && bucket.getRecordCount() > 0) {
                this.expandBucketForPartitionValue(jobId, query.isIncludeInterim(),
                        bucket, query.getPartitionValue());
            }

            bucket.setAnomalyScore(
                    bucket.partitionAnomalyScore(query.getPartitionValue()));
        }
        return new QueryPage<>(Collections.singletonList(bucket), 1, Bucket.RESULTS_FIELD);
    }

    final class ScoreTimestamp
    {
        double score;
        Date timestamp;

        public ScoreTimestamp(Date timestamp, double score)
        {
            this.score = score;
            this.timestamp = timestamp;
        }
    }

    private List<ScoreTimestamp> partitionScores(String jobId, Object epochStart,
            Object epochEnd, String partitionFieldValue)
                    throws ResourceNotFoundException
    {
        QueryBuilder qb = new ResultsFilterBuilder()
                .timeRange(ElasticsearchMappings.ES_TIMESTAMP, epochStart, epochEnd)
                .build();

        FieldSortBuilder sb = new FieldSortBuilder(ElasticsearchMappings.ES_TIMESTAMP)
                .order(SortOrder.ASC);
        String indexName = JobResultsPersister.getJobIndexName(jobId);
        SearchRequestBuilder searchBuilder = client
                .prepareSearch(indexName)
                .setQuery(qb)
                .addSort(sb)
                .setTypes(ReservedFieldNames.PARTITION_NORMALIZED_PROB_TYPE);

        SearchResponse searchResponse;
        try {
            searchResponse = searchBuilder.get();
        } catch (IndexNotFoundException e) {
            throw ExceptionsHelper.missingJobException(jobId);
        }

        List<ScoreTimestamp> results = new ArrayList<>();

        // expect 1 document per bucket
        if (searchResponse.getHits().totalHits() > 0)
        {

            Map<String, Object> m  = searchResponse.getHits().getAt(0).getSource();

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> probs = (List<Map<String, Object>>)
            m.get(ReservedFieldNames.PARTITION_NORMALIZED_PROBS);
            for (Map<String, Object> prob : probs)
            {
                if (partitionFieldValue.equals(prob.get(AnomalyRecord.PARTITION_FIELD_VALUE.getPreferredName())))
                {
                    Date ts = new Date(TimeUtils.dateStringToEpoch((String) m.get(ElasticsearchMappings.ES_TIMESTAMP)));
                    results.add(new ScoreTimestamp(ts,
                            (Double) prob.get(Bucket.MAX_NORMALIZED_PROBABILITY.getPreferredName())));
                }
            }
        }

        return results;
    }

    public int expandBucketForPartitionValue(String jobId, boolean includeInterim, Bucket bucket,
            String partitionFieldValue) throws ResourceNotFoundException
    {
        int from = 0;

        QueryPage<AnomalyRecord> page = bucketRecords(
                jobId, bucket, from, RECORDS_SIZE_PARAM, includeInterim,
                AnomalyRecord.PROBABILITY.getPreferredName(), false, partitionFieldValue);
        bucket.setRecords(page.results());

        while (page.count() > from + RECORDS_SIZE_PARAM)
        {
            from += RECORDS_SIZE_PARAM;
            page = bucketRecords(
                    jobId, bucket, from, RECORDS_SIZE_PARAM, includeInterim,
                    AnomalyRecord.PROBABILITY.getPreferredName(), false, partitionFieldValue);
            bucket.getRecords().addAll(page.results());
        }

        return bucket.getRecords().size();
    }


    @Override
    public BatchedDocumentsIterator<Bucket> newBatchedBucketsIterator(String jobId)
    {
        return new ElasticsearchBatchedBucketsIterator(client, jobId, parseFieldMatcher);
    }

    @Override
    public int expandBucket(String jobId, boolean includeInterim, Bucket bucket) throws ResourceNotFoundException {
        int from = 0;

        QueryPage<AnomalyRecord> page = bucketRecords(
                jobId, bucket, from, RECORDS_SIZE_PARAM, includeInterim,
                AnomalyRecord.PROBABILITY.getPreferredName(), false, null);
        bucket.setRecords(page.results());

        while (page.count() > from + RECORDS_SIZE_PARAM)
        {
            from += RECORDS_SIZE_PARAM;
            page = bucketRecords(
                    jobId, bucket, from, RECORDS_SIZE_PARAM, includeInterim,
                    AnomalyRecord.PROBABILITY.getPreferredName(), false, null);
            bucket.getRecords().addAll(page.results());
        }

        return bucket.getRecords().size();
    }

    QueryPage<AnomalyRecord> bucketRecords(String jobId,
            Bucket bucket, int from, int size, boolean includeInterim,
            String sortField, boolean descending, String partitionFieldValue)
                    throws ResourceNotFoundException
    {
        // Find the records using the time stamp rather than a parent-child
        // relationship.  The parent-child filter involves two queries behind
        // the scenes, and Elasticsearch documentation claims it's significantly
        // slower.  Here we rely on the record timestamps being identical to the
        // bucket timestamp.
        QueryBuilder recordFilter = QueryBuilders.termQuery(ElasticsearchMappings.ES_TIMESTAMP, bucket.getTimestamp().getTime());

        recordFilter = new ResultsFilterBuilder(recordFilter)
                .interim(AnomalyRecord.IS_INTERIM.getPreferredName(), includeInterim)
                .term(AnomalyRecord.PARTITION_FIELD_VALUE.getPreferredName(), partitionFieldValue)
                .build();

        FieldSortBuilder sb = null;
        if (sortField != null)
        {
            sb = new FieldSortBuilder(esSortField(sortField))
                    .missing("_last")
                    .order(descending ? SortOrder.DESC : SortOrder.ASC);
        }

        return records(jobId, from, size, recordFilter, sb, SECONDARY_SORT,
                descending);
    }

    @Override
    public QueryPage<CategoryDefinition> categoryDefinitions(String jobId, int from, int size) {
        String indexName = JobResultsPersister.getJobIndexName(jobId);
        LOGGER.trace("ES API CALL: search all of type {} from index {} sort ascending {} from {} size {}",
                CategoryDefinition.TYPE.getPreferredName(), indexName, CategoryDefinition.CATEGORY_ID.getPreferredName(), from, size);

        SearchRequestBuilder searchBuilder = client.prepareSearch(indexName)
                .setTypes(CategoryDefinition.TYPE.getPreferredName())
                .setFrom(from).setSize(size)
                .addSort(new FieldSortBuilder(CategoryDefinition.CATEGORY_ID.getPreferredName()).order(SortOrder.ASC));

        SearchResponse searchResponse;
        try {
            searchResponse = searchBuilder.get();
        } catch (IndexNotFoundException e) {
            throw ExceptionsHelper.missingJobException(jobId);
        }
        SearchHit[] hits = searchResponse.getHits().getHits();
        List<CategoryDefinition> results = new ArrayList<>(hits.length);
        for (SearchHit hit : hits) {
            BytesReference source = hit.getSourceRef();
            XContentParser parser;
            try {
                parser = XContentFactory.xContent(source).createParser(source);
            } catch (IOException e) {
                throw new ElasticsearchParseException("failed to parser category definition", e);
            }
            CategoryDefinition categoryDefinition = CategoryDefinition.PARSER.apply(parser, () -> parseFieldMatcher);
            results.add(categoryDefinition);
        }

        return new QueryPage<>(results, searchResponse.getHits().getTotalHits(), CategoryDefinition.RESULTS_FIELD);
    }


    @Override
    public QueryPage<CategoryDefinition> categoryDefinition(String jobId, String categoryId) {
        String indexName = JobResultsPersister.getJobIndexName(jobId);
        GetResponse response;

        try {
            LOGGER.trace("ES API CALL: get ID {} type {} from index {}",
                    categoryId, CategoryDefinition.TYPE, indexName);

            response = client.prepareGet(indexName, CategoryDefinition.TYPE.getPreferredName(), categoryId).get();
        } catch (IndexNotFoundException e) {
            throw ExceptionsHelper.missingJobException(jobId);
        }


        if (response.isExists()) {
            BytesReference source = response.getSourceAsBytesRef();
            XContentParser parser;
            try {
                parser = XContentFactory.xContent(source).createParser(source);
            } catch (IOException e) {
                throw new ElasticsearchParseException("failed to parser category definition", e);
            }
            CategoryDefinition definition = CategoryDefinition.PARSER.apply(parser, () -> parseFieldMatcher);
            return new QueryPage<>(Collections.singletonList(definition), 1, CategoryDefinition.RESULTS_FIELD);
        }

        throw QueryPage.emptyQueryPage(Bucket.RESULTS_FIELD);
    }

    @Override
    public QueryPage<AnomalyRecord> records(String jobId, RecordsQueryBuilder.RecordsQuery query)
            throws ResourceNotFoundException {
        QueryBuilder fb = new ResultsFilterBuilder()
                .timeRange(ElasticsearchMappings.ES_TIMESTAMP, query.getEpochStart(), query.getEpochEnd())
                .score(AnomalyRecord.ANOMALY_SCORE.getPreferredName(), query.getAnomalyScoreThreshold())
                .score(AnomalyRecord.NORMALIZED_PROBABILITY.getPreferredName(), query.getNormalizedProbabilityThreshold())
                .interim(AnomalyRecord.IS_INTERIM.getPreferredName(), query.isIncludeInterim())
                .term(AnomalyRecord.PARTITION_FIELD_VALUE.getPreferredName(), query.getPartitionFieldValue()).build();

        return records(jobId, query.getFrom(), query.getSize(), fb, query.getSortField(), query.isSortDescending());
    }


    private QueryPage<AnomalyRecord> records(String jobId,
            int from, int size, QueryBuilder recordFilter,
            String sortField, boolean descending)
                    throws ResourceNotFoundException
    {
        FieldSortBuilder sb = null;
        if (sortField != null)
        {
            sb = new FieldSortBuilder(esSortField(sortField))
                    .missing("_last")
                    .order(descending ? SortOrder.DESC : SortOrder.ASC);
        }

        return records(jobId, from, size, recordFilter, sb, SECONDARY_SORT, descending);
    }


    /**
     * The returned records have their id set.
     */
    private QueryPage<AnomalyRecord> records(String jobId, int from, int size,
            QueryBuilder recordFilter, FieldSortBuilder sb, List<String> secondarySort,
            boolean descending) throws ResourceNotFoundException {
        String indexName = JobResultsPersister.getJobIndexName(jobId);

        recordFilter = new BoolQueryBuilder()
                .filter(recordFilter)
                .filter(new TermsQueryBuilder(Result.RESULT_TYPE.getPreferredName(), AnomalyRecord.RESULT_TYPE_VALUE));

        SearchRequestBuilder searchBuilder = client.prepareSearch(indexName)
                .setTypes(Result.TYPE.getPreferredName())
                .setQuery(recordFilter)
                .setFrom(from).setSize(size)
                .addSort(sb == null ? SortBuilders.fieldSort(ElasticsearchMappings.ES_DOC) : sb)
                .setFetchSource(true);  // the field option turns off source so request it explicitly

        for (String sortField : secondarySort)
        {
            searchBuilder.addSort(esSortField(sortField), descending ? SortOrder.DESC : SortOrder.ASC);
        }

        SearchResponse searchResponse;
        try {
            LOGGER.trace("ES API CALL: search all of result type {} from index {}{}{}  with filter after sort from {} size {}",
                    AnomalyRecord.RESULT_TYPE_VALUE, indexName, (sb != null) ? " with sort"  : "",
                    secondarySort.isEmpty() ? "" : " with secondary sort", from, size);

            searchResponse = searchBuilder.get();
        } catch (IndexNotFoundException e) {
            throw ExceptionsHelper.missingJobException(jobId);
        }

        List<AnomalyRecord> results = new ArrayList<>();
        for (SearchHit hit : searchResponse.getHits().getHits())
        {
            BytesReference source = hit.getSourceRef();
            XContentParser parser;
            try {
                parser = XContentFactory.xContent(source).createParser(source);
            } catch (IOException e) {
                throw new ElasticsearchParseException("failed to parser records", e);
            }
            AnomalyRecord record = AnomalyRecord.PARSER.apply(parser, () -> parseFieldMatcher);

            // set the ID
            record.setId(hit.getId());
            results.add(record);
        }

        return new QueryPage<>(results, searchResponse.getHits().getTotalHits(), AnomalyRecord.RESULTS_FIELD);
    }

    @Override
    public QueryPage<Influencer> influencers(String jobId, InfluencersQuery query) throws ResourceNotFoundException
    {

        QueryBuilder fb = new ResultsFilterBuilder()
                .timeRange(ElasticsearchMappings.ES_TIMESTAMP, query.getEpochStart(), query.getEpochEnd())
                .score(Bucket.ANOMALY_SCORE.getPreferredName(), query.getAnomalyScoreFilter())
                .interim(Bucket.IS_INTERIM.getPreferredName(), query.isIncludeInterim())
                .build();

        return influencers(jobId, query.getFrom(), query.getSize(), fb, query.getSortField(),
                query.isSortDescending());
    }

    private QueryPage<Influencer> influencers(String jobId, int from, int size, QueryBuilder filterBuilder, String sortField,
            boolean sortDescending) throws ResourceNotFoundException {
        String indexName = JobResultsPersister.getJobIndexName(jobId);
        LOGGER.trace("ES API CALL: search all of result type {} from index {}{}  with filter from {} size {}",
                () -> Influencer.RESULT_TYPE_VALUE, () -> indexName,
                () -> (sortField != null) ?
                        " with sort " + (sortDescending ? "descending" : "ascending") + " on field " + esSortField(sortField) : "",
                () -> from, () -> size);

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexName)
                .setTypes(Result.TYPE.getPreferredName())
                .setQuery(filterBuilder)
                .setFrom(from).setSize(size);

        FieldSortBuilder sb = sortField == null ? SortBuilders.fieldSort(ElasticsearchMappings.ES_DOC)
                : new FieldSortBuilder(esSortField(sortField)).order(sortDescending ? SortOrder.DESC : SortOrder.ASC);
        searchRequestBuilder.addSort(sb);

        SearchResponse response = null;
        try
        {
            response = searchRequestBuilder.get();
        }
        catch (IndexNotFoundException e)
        {
            throw new ResourceNotFoundException("job " + jobId + " not found");
        }

        List<Influencer> influencers = new ArrayList<>();
        for (SearchHit hit : response.getHits().getHits())
        {
            BytesReference source = hit.getSourceRef();
            XContentParser parser;
            try {
                parser = XContentFactory.xContent(source).createParser(source);
            } catch (IOException e) {
                throw new ElasticsearchParseException("failed to parser list", e);
            }
            Influencer influencer = Influencer.PARSER.apply(parser, () -> parseFieldMatcher);
            influencer.setId(hit.getId());

            influencers.add(influencer);
        }

        return new QueryPage<>(influencers, response.getHits().getTotalHits(), Influencer.RESULTS_FIELD);
    }

    @Override
    public Optional<Influencer> influencer(String jobId, String influencerId)
    {
        throw new IllegalStateException();
    }

    @Override
    public BatchedDocumentsIterator<Influencer> newBatchedInfluencersIterator(String jobId)
    {
        return new ElasticsearchBatchedInfluencersIterator(client, jobId, parseFieldMatcher);
    }

    @Override
    public BatchedDocumentsIterator<ModelSnapshot> newBatchedModelSnapshotIterator(String jobId)
    {
        return new ElasticsearchBatchedModelSnapshotIterator(client, jobId, parseFieldMatcher);
    }

    @Override
    public BatchedDocumentsIterator<ModelDebugOutput> newBatchedModelDebugOutputIterator(String jobId)
    {
        return new ElasticsearchBatchedModelDebugOutputIterator(client, jobId, parseFieldMatcher);
    }

    @Override
    public BatchedDocumentsIterator<ModelSizeStats> newBatchedModelSizeStatsIterator(String jobId)
    {
        return new ElasticsearchBatchedModelSizeStatsIterator(client, jobId, parseFieldMatcher);
    }

    @Override
    public Optional<Quantiles> getQuantiles(String jobId)
    {
        String indexName = JobResultsPersister.getJobIndexName(jobId);
        try
        {
            LOGGER.trace("ES API CALL: get ID " + Quantiles.QUANTILES_ID +
                    " type " + Quantiles.TYPE + " from index " + indexName);
            GetResponse response = client.prepareGet(
                    indexName, Quantiles.TYPE.getPreferredName(), Quantiles.QUANTILES_ID).get();
            if (!response.isExists())
            {
                LOGGER.info("There are currently no quantiles for job " + jobId);
                return Optional.empty();
            }
            return Optional.of(createQuantiles(jobId, response));
        }
        catch (IndexNotFoundException e)
        {
            LOGGER.error("Missing index when getting quantiles", e);
            throw e;
        }
    }

    @Override
    public QueryPage<ModelSnapshot> modelSnapshots(String jobId, int from, int size)
    {
        return modelSnapshots(jobId, from, size, null, null, null, true, null, null);
    }

    @Override
    public QueryPage<ModelSnapshot> modelSnapshots(String jobId, int from, int size,
                                                   String startEpochMs, String endEpochMs, String sortField, boolean sortDescending,
                                                   String snapshotId, String description)
    {
        boolean haveId = snapshotId != null && !snapshotId.isEmpty();
        boolean haveDescription = description != null && !description.isEmpty();
        ResultsFilterBuilder fb;
        if (haveId || haveDescription)
        {
            BoolQueryBuilder query = QueryBuilders.boolQuery();
            if (haveId)
            {
                query.must(QueryBuilders.termQuery(ModelSnapshot.SNAPSHOT_ID.getPreferredName(), snapshotId));
            }
            if (haveDescription)
            {
                query.must(QueryBuilders.termQuery(ModelSnapshot.DESCRIPTION.getPreferredName(), description));
            }

            fb = new ResultsFilterBuilder(query);
        }
        else
        {
            fb = new ResultsFilterBuilder();
        }

        return modelSnapshots(jobId, from, size,
                (sortField == null || sortField.isEmpty()) ? ModelSnapshot.RESTORE_PRIORITY.getPreferredName() : sortField,
                        sortDescending, fb.timeRange(
                                ElasticsearchMappings.ES_TIMESTAMP, startEpochMs, endEpochMs).build());
    }

    private QueryPage<ModelSnapshot> modelSnapshots(String jobId, int from, int size,
            String sortField, boolean sortDescending, QueryBuilder fb)
    {
        FieldSortBuilder sb = new FieldSortBuilder(esSortField(sortField))
                .order(sortDescending ? SortOrder.DESC : SortOrder.ASC);

        // Wrap in a constant_score because we always want to
        // run it as a filter
        fb = new ConstantScoreQueryBuilder(fb);

        SearchResponse searchResponse;
        try
        {
            String indexName = JobResultsPersister.getJobIndexName(jobId);
            LOGGER.trace("ES API CALL: search all of type " + ModelSnapshot.TYPE +
                    " from index " + indexName + " sort ascending " + esSortField(sortField) +
                    " with filter after sort from " + from + " size " + size);
            searchResponse = client.prepareSearch(indexName)
                    .setTypes(ModelSnapshot.TYPE.getPreferredName())
                    .addSort(sb)
                    .setQuery(fb)
                    .setFrom(from).setSize(size)
                    .get();
        }
        catch (IndexNotFoundException e)
        {
            LOGGER.error("Failed to read modelSnapshots", e);
            throw e;
        }

        List<ModelSnapshot> results = new ArrayList<>();

        for (SearchHit hit : searchResponse.getHits().getHits())
        {
            // Remove the Kibana/Logstash '@timestamp' entry as stored in Elasticsearch,
            // and replace using the API 'timestamp' key.
            Object timestamp = hit.getSource().remove(ElasticsearchMappings.ES_TIMESTAMP);
            hit.getSource().put(ModelSnapshot.TIMESTAMP.getPreferredName(), timestamp);

            Object o = hit.getSource().get(ModelSizeStats.TYPE.getPreferredName());
            if (o instanceof Map)
            {
                @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>)o;
                Object ts = map.remove(ElasticsearchMappings.ES_TIMESTAMP);
                map.put(ModelSizeStats.TIMESTAMP_FIELD.getPreferredName(), ts);
            }

            BytesReference source = hit.getSourceRef();
            XContentParser parser;
            try {
                parser = XContentFactory.xContent(source).createParser(source);
            } catch (IOException e) {
                throw new ElasticsearchParseException("failed to parser list", e);
            }
            ModelSnapshot modelSnapshot = ModelSnapshot.PARSER.apply(parser, () -> parseFieldMatcher);
            results.add(modelSnapshot);
        }

        return new QueryPage<>(results, searchResponse.getHits().getTotalHits(), ModelSnapshot.RESULTS_FIELD);
    }

    public void restoreStateToStream(String jobId, ModelSnapshot modelSnapshot, OutputStream restoreStream) throws IOException {
        String indexName = JobResultsPersister.getJobIndexName(jobId);

        // First try to restore categorizer state.  There are no snapshots for this, so the IDs simply
        // count up until a document is not found.  It's NOT an error to have no categorizer state.
        int docNum = 0;
        while (true) {
            String docId = Integer.toString(++docNum);

            LOGGER.trace("ES API CALL: get ID {} type {} from index {}", docId, CategorizerState.TYPE, indexName);

            GetResponse stateResponse = client.prepareGet(indexName, CategorizerState.TYPE, docId).get();
            if (!stateResponse.isExists()) {
                break;
            }
            writeStateToStream(stateResponse.getSourceAsBytesRef(), restoreStream);
        }

        // Finally try to restore model state.  This must come after categorizer state because that's
        // the order the C++ process expects.
        int numDocs = modelSnapshot.getSnapshotDocCount();
        for (docNum = 1; docNum <= numDocs; ++docNum) {
            String docId = String.format(Locale.ROOT, "%s_%d", modelSnapshot.getSnapshotId(), docNum);

            LOGGER.trace("ES API CALL: get ID {} type {} from index {}", docId, ModelState.TYPE, indexName);

            GetResponse stateResponse = client.prepareGet(indexName, ModelState.TYPE, docId).get();
            if (!stateResponse.isExists()) {
                LOGGER.error("Expected {} documents for model state for {} snapshot {} but failed to find {}",
                        numDocs, jobId, modelSnapshot.getSnapshotId(), docId);
                break;
            }
            writeStateToStream(stateResponse.getSourceAsBytesRef(), restoreStream);
        }
    }

    private void writeStateToStream(BytesReference source, OutputStream stream) throws IOException {
        // The source bytes are already UTF-8.  The C++ process wants UTF-8, so we
        // can avoid converting to a Java String only to convert back again.
        BytesRefIterator iterator = source.iterator();
        for (BytesRef ref = iterator.next(); ref != null; ref = iterator.next()) {
            // There's a complication that the source can already have trailing 0 bytes
            int length = ref.bytes.length;
            while (length > 0 && ref.bytes[length - 1] == 0) {
                --length;
            }
            if (length > 0) {
                stream.write(ref.bytes, 0, length);
            }
        }
        // This is dictated by RapidJSON on the C++ side; it treats a '\0' as end-of-file
        // even when it's not really end-of-file, and this is what we need because we're
        // sending multiple JSON documents via the same named pipe.
        stream.write(0);
    }

    private Quantiles createQuantiles(String jobId, GetResponse response) {
        BytesReference source = response.getSourceAsBytesRef();
        XContentParser parser;
        try {
            parser = XContentFactory.xContent(source).createParser(source);
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to parser quantiles", e);
        }
        Quantiles quantiles = Quantiles.PARSER.apply(parser, () -> parseFieldMatcher);
        if (quantiles.getQuantileState() == null)
        {
            LOGGER.error("Inconsistency - no " + Quantiles.QUANTILE_STATE
                    + " field in quantiles for job " + jobId);
        }
        return quantiles;
    }

    @Override
    public Optional<ModelSizeStats> modelSizeStats(String jobId) {
        String indexName = JobResultsPersister.getJobIndexName(jobId);
        try {
            LOGGER.trace("ES API CALL: get ID " + ModelSizeStats.TYPE +
                    " type " + ModelSizeStats.TYPE + " from index " + indexName);

            GetResponse modelSizeStatsResponse = client.prepareGet(
                    indexName, ModelSizeStats.TYPE.getPreferredName(), ModelSizeStats.TYPE.getPreferredName()).get();

            if (!modelSizeStatsResponse.isExists()) {
                String msg = "No memory usage details for job with id " + jobId;
                LOGGER.warn(msg);
                return Optional.empty();
            } else {
                BytesReference source = modelSizeStatsResponse.getSourceAsBytesRef();
                XContentParser parser;
                try {
                    parser = XContentFactory.xContent(source).createParser(source);
                } catch (IOException e) {
                    throw new ElasticsearchParseException("failed to parser model size stats", e);
                }
                ModelSizeStats modelSizeStats = ModelSizeStats.PARSER.apply(parser, () -> parseFieldMatcher).build();
                return Optional.of(modelSizeStats);
            }
        } catch (IndexNotFoundException e) {
            LOGGER.warn("Missing index " + indexName, e);
            return Optional.empty();
        }
    }

    @Override
    public Optional<ListDocument> getList(String listId) {
        GetResponse response = client.prepareGet(PRELERT_INFO_INDEX, ListDocument.TYPE.getPreferredName(), listId).get();
        if (!response.isExists())
        {
            return Optional.empty();
        }
        BytesReference source = response.getSourceAsBytesRef();
        XContentParser parser;
        try {
            parser = XContentFactory.xContent(source).createParser(source);
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to parser list", e);
        }
        ListDocument listDocument = ListDocument.PARSER.apply(parser, () -> parseFieldMatcher);
        return Optional.of(listDocument);
    }

    @Override
    public Auditor audit(String jobId)
    {
        // NORELEASE Create proper auditor or remove
        // return new ElasticsearchAuditor(client, PRELERT_INFO_INDEX, jobId);
        return new Auditor() {
            @Override
            public void info(String message) {
            }

            @Override
            public void warning(String message) {
            }

            @Override
            public void error(String message) {
            }

            @Override
            public void activity(String message) {
            }

            @Override
            public void activity(int totalJobs, int totalDetectors, int runningJobs, int runningDetectors) {
            }
        };

    }

    private String esSortField(String sortField)
    {
        // Beware: There's an assumption here that Bucket.TIMESTAMP,
        // AnomalyRecord.TIMESTAMP, Influencer.TIMESTAMP and
        // ModelSnapshot.TIMESTAMP are all the same
        return sortField.equals(Bucket.TIMESTAMP.getPreferredName()) ? ElasticsearchMappings.ES_TIMESTAMP : sortField;
    }
}
