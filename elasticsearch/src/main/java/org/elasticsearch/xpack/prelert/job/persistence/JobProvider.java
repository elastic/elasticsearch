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
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
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
import org.elasticsearch.xpack.prelert.job.results.PerPartitionMaxProbabilities;
import org.elasticsearch.xpack.prelert.job.results.Result;
import org.elasticsearch.xpack.prelert.job.usage.Usage;
import org.elasticsearch.xpack.prelert.lists.ListDocument;
import org.elasticsearch.xpack.prelert.utils.ExceptionsHelper;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;

public class JobProvider {
    private static final Logger LOGGER = Loggers.getLogger(JobProvider.class);

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

    public JobProvider(Client client, int numberOfReplicas, ParseFieldMatcher parseFieldMatcher) {
        this.parseFieldMatcher = parseFieldMatcher;
        this.client = Objects.requireNonNull(client);
        this.numberOfReplicas = numberOfReplicas;
    }

    /**
     * If the {@value JobProvider#PRELERT_USAGE_INDEX} index does
     * not exist then create it here with the usage document mapping.
     */
    public void createUsageMeteringIndex(BiConsumer<Boolean, Exception> listener) {
        try {
            LOGGER.info("Creating the internal '{}' index", PRELERT_USAGE_INDEX);
            XContentBuilder usageMapping = ElasticsearchMappings.usageMapping();
            LOGGER.trace("ES API CALL: create index {}", PRELERT_USAGE_INDEX);
            client.admin().indices().prepareCreate(PRELERT_USAGE_INDEX)
                    .setSettings(prelertIndexSettings())
                    .addMapping(Usage.TYPE, usageMapping)
                    .execute(new ActionListener<CreateIndexResponse>() {
                        @Override
                        public void onResponse(CreateIndexResponse createIndexResponse) {
                            listener.accept(true, null);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            listener.accept(false, e);
                        }
                    });

        } catch (IOException e) {
            LOGGER.warn("Error checking the usage metering index", e);
        }
    }

    /**
     * Build the Elasticsearch index settings that we want to apply to Prelert
     * indexes.  It's better to do this in code rather than in elasticsearch.yml
     * because then the settings can be applied regardless of whether we're
     * using our own Elasticsearch to store results or a customer's pre-existing
     * Elasticsearch.
     *
     * @return An Elasticsearch builder initialised with the desired settings
     * for Prelert indexes.
     */
    private Settings.Builder prelertIndexSettings() {
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
    // TODO: rename and move?
    public void createJobRelatedIndices(Job job, ActionListener<Boolean> listener) {
        Collection<String> termFields = (job.getAnalysisConfig() != null) ? job.getAnalysisConfig().termFields() : Collections.emptyList();
        try {
            XContentBuilder resultsMapping = ElasticsearchMappings.resultsMapping(termFields);
            XContentBuilder categorizerStateMapping = ElasticsearchMappings.categorizerStateMapping();
            XContentBuilder categoryDefinitionMapping = ElasticsearchMappings.categoryDefinitionMapping();
            XContentBuilder quantilesMapping = ElasticsearchMappings.quantilesMapping();
            XContentBuilder modelStateMapping = ElasticsearchMappings.modelStateMapping();
            XContentBuilder modelSnapshotMapping = ElasticsearchMappings.modelSnapshotMapping();
            XContentBuilder dataCountsMapping = ElasticsearchMappings.dataCountsMapping();

            String jobId = job.getId();
            LOGGER.trace("ES API CALL: create index {}", job.getId());
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(JobResultsPersister.getJobIndexName(jobId));
            createIndexRequest.settings(prelertIndexSettings());
            createIndexRequest.mapping(Result.TYPE.getPreferredName(), resultsMapping);
            createIndexRequest.mapping(CategorizerState.TYPE, categorizerStateMapping);
            createIndexRequest.mapping(CategoryDefinition.TYPE.getPreferredName(), categoryDefinitionMapping);
            createIndexRequest.mapping(Quantiles.TYPE.getPreferredName(), quantilesMapping);
            createIndexRequest.mapping(ModelState.TYPE.getPreferredName(), modelStateMapping);
            createIndexRequest.mapping(ModelSnapshot.TYPE.getPreferredName(), modelSnapshotMapping);
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

    /**
     * Delete all the job related documents from the database.
     */
    // TODO: should live together with createJobRelatedIndices (in case it moves)?
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

    /**
     * Get the job's data counts
     * @param jobId The job id
     * @return The dataCounts or default constructed object if not found
     */
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
                    throw new ElasticsearchParseException("failed to parse bucket", e);
                }
            }

        } catch (IndexNotFoundException e) {
            throw ExceptionsHelper.missingJobException(jobId);
        }
    }

    /**
     * Search for buckets with the parameters in the {@link BucketsQueryBuilder}
     * @return QueryPage of Buckets
     * @throws ResourceNotFoundException If the job id is no recognised
     */
    public QueryPage<Bucket> buckets(String jobId, BucketsQuery query)
            throws ResourceNotFoundException {
        QueryBuilder fb = new ResultsFilterBuilder()
                .timeRange(Bucket.TIMESTAMP.getPreferredName(), query.getStart(), query.getEnd())
                .score(Bucket.ANOMALY_SCORE.getPreferredName(), query.getAnomalyScoreFilter())
                .score(Bucket.MAX_NORMALIZED_PROBABILITY.getPreferredName(), query.getNormalizedProbability())
                .interim(Bucket.IS_INTERIM.getPreferredName(), query.isIncludeInterim())
                .build();

        SortBuilder<?> sortBuilder = new FieldSortBuilder(query.getSortField())
                .order(query.isSortDescending() ? SortOrder.DESC : SortOrder.ASC);

        QueryPage<Bucket> buckets = buckets(jobId, query.isIncludeInterim(), query.getFrom(), query.getSize(), fb, sortBuilder);

        if (Strings.isNullOrEmpty(query.getPartitionValue())) {
            for (Bucket b : buckets.results()) {
                if (query.isExpand() && b.getRecordCount() > 0) {
                    expandBucket(jobId, query.isIncludeInterim(), b);
                }
            }
        } else {
            List<PerPartitionMaxProbabilities> scores =
                    partitionMaxNormalizedProbabilities(jobId, query.getStart(), query.getEnd(), query.getPartitionValue());

            mergePartitionScoresIntoBucket(scores, buckets.results(), query.getPartitionValue());

            for (Bucket b : buckets.results()) {
                if (query.isExpand() && b.getRecordCount() > 0) {
                    this.expandBucketForPartitionValue(jobId, query.isIncludeInterim(), b, query.getPartitionValue());
                }

                b.setAnomalyScore(b.partitionAnomalyScore(query.getPartitionValue()));
            }
        }

        return buckets;
    }

    void mergePartitionScoresIntoBucket(List<PerPartitionMaxProbabilities> partitionProbs, List<Bucket> buckets, String partitionValue) {
        Iterator<PerPartitionMaxProbabilities> itr = partitionProbs.iterator();
        PerPartitionMaxProbabilities partitionProb = itr.hasNext() ? itr.next() : null;
        for (Bucket b : buckets) {
            if (partitionProb == null) {
                b.setMaxNormalizedProbability(0.0);
            } else {
                if (partitionProb.getTimestamp().equals(b.getTimestamp())) {
                    b.setMaxNormalizedProbability(partitionProb.getMaxProbabilityForPartition(partitionValue));
                    partitionProb = itr.hasNext() ? itr.next() : null;
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
                throw new ElasticsearchParseException("failed to parse bucket", e);
            }
            Bucket bucket = Bucket.PARSER.apply(parser, () -> parseFieldMatcher);

            if (includeInterim || bucket.isInterim() == false) {
                results.add(bucket);
            }
        }

        return new QueryPage<>(results, searchResponse.getHits().getTotalHits(), Bucket.RESULTS_FIELD);
    }

    /**
     * Get the bucket at time <code>timestampMillis</code> from the job.
     *
     * @param jobId the job id
     * @param query The bucket query
     * @return QueryPage Bucket
     * @throws ResourceNotFoundException If the job id is not recognised
     */
    public QueryPage<Bucket> bucket(String jobId, BucketQueryBuilder.BucketQuery query) throws ResourceNotFoundException {
        String indexName = JobResultsPersister.getJobIndexName(jobId);
        SearchHits hits;
        try {
            LOGGER.trace("ES API CALL: get Bucket with timestamp {} from index {}", query.getTimestamp(), indexName);
            QueryBuilder matchQuery = QueryBuilders.matchQuery(Bucket.TIMESTAMP.getPreferredName(), query.getTimestamp());

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
            throw new ElasticsearchParseException("failed to parse bucket", e);
        }
        Bucket bucket = Bucket.PARSER.apply(parser, () -> parseFieldMatcher);

        // don't return interim buckets if not requested
        if (bucket.isInterim() && query.isIncludeInterim() == false) {
            throw QueryPage.emptyQueryPage(Bucket.RESULTS_FIELD);
        }

        if (Strings.isNullOrEmpty(query.getPartitionValue())) {
            if (query.isExpand() && bucket.getRecordCount() > 0) {
                expandBucket(jobId, query.isIncludeInterim(), bucket);
            }
        } else {
            List<PerPartitionMaxProbabilities> partitionProbs =
                    partitionMaxNormalizedProbabilities(jobId, query.getTimestamp(), query.getTimestamp() + 1, query.getPartitionValue());

            if (partitionProbs.size() > 1) {
                LOGGER.error("Found more than one PerPartitionMaxProbabilities with timestamp [" + query.getTimestamp() + "]" +
                        " from index " + indexName);
            }
            if (partitionProbs.size() > 0) {
                bucket.setMaxNormalizedProbability(partitionProbs.get(0).getMaxProbabilityForPartition(query.getPartitionValue()));
            }

            if (query.isExpand() && bucket.getRecordCount() > 0) {
                this.expandBucketForPartitionValue(jobId, query.isIncludeInterim(),
                        bucket, query.getPartitionValue());
            }

            bucket.setAnomalyScore(
                    bucket.partitionAnomalyScore(query.getPartitionValue()));
        }
        return new QueryPage<>(Collections.singletonList(bucket), 1, Bucket.RESULTS_FIELD);
    }


    private List<PerPartitionMaxProbabilities> partitionMaxNormalizedProbabilities(String jobId, Object epochStart, Object epochEnd,
                                                                                   String partitionFieldValue)
                    throws ResourceNotFoundException {
        QueryBuilder timeRangeQuery = new ResultsFilterBuilder()
                .timeRange(Bucket.TIMESTAMP.getPreferredName(), epochStart, epochEnd)
                .build();

        QueryBuilder boolQuery = new BoolQueryBuilder()
                .filter(timeRangeQuery)
                .filter(new TermsQueryBuilder(Result.RESULT_TYPE.getPreferredName(), PerPartitionMaxProbabilities.RESULT_TYPE_VALUE))
                .filter(new TermsQueryBuilder(AnomalyRecord.PARTITION_FIELD_VALUE.getPreferredName(), partitionFieldValue));

        FieldSortBuilder sb = new FieldSortBuilder(Bucket.TIMESTAMP.getPreferredName()).order(SortOrder.ASC);
        String indexName = JobResultsPersister.getJobIndexName(jobId);
        SearchRequestBuilder searchBuilder = client
                .prepareSearch(indexName)
                .setQuery(boolQuery)
                .addSort(sb)
                .setTypes(Result.TYPE.getPreferredName());

        SearchResponse searchResponse;
        try {
            searchResponse = searchBuilder.get();
        } catch (IndexNotFoundException e) {
            throw ExceptionsHelper.missingJobException(jobId);
        }

        List<PerPartitionMaxProbabilities> results = new ArrayList<>();

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            BytesReference source = hit.getSourceRef();
            XContentParser parser;
            try {
                parser = XContentFactory.xContent(source).createParser(source);
            } catch (IOException e) {
                throw new ElasticsearchParseException("failed to parse PerPartitionMaxProbabilities", e);
            }

            results.add(PerPartitionMaxProbabilities.PARSER.apply(parser, () -> parseFieldMatcher));
        }

        return results;
    }

    public int expandBucketForPartitionValue(String jobId, boolean includeInterim, Bucket bucket,
                                             String partitionFieldValue) throws ResourceNotFoundException {
        int from = 0;

        QueryPage<AnomalyRecord> page = bucketRecords(
                jobId, bucket, from, RECORDS_SIZE_PARAM, includeInterim,
                AnomalyRecord.PROBABILITY.getPreferredName(), false, partitionFieldValue);
        bucket.setRecords(page.results());

        while (page.count() > from + RECORDS_SIZE_PARAM) {
            from += RECORDS_SIZE_PARAM;
            page = bucketRecords(
                    jobId, bucket, from, RECORDS_SIZE_PARAM, includeInterim,
                    AnomalyRecord.PROBABILITY.getPreferredName(), false, partitionFieldValue);
            bucket.getRecords().addAll(page.results());
        }

        return bucket.getRecords().size();
    }


    /**
     * Returns a {@link BatchedDocumentsIterator} that allows querying
     * and iterating over a large number of buckets of the given job
     *
     * @param jobId the id of the job for which buckets are requested
     * @return a bucket {@link BatchedDocumentsIterator}
     */
    public BatchedDocumentsIterator<Bucket> newBatchedBucketsIterator(String jobId) {
        return new ElasticsearchBatchedBucketsIterator(client, jobId, parseFieldMatcher);
    }

    /**
     * Expand a bucket to include the associated records.
     *
     * @param jobId the job id
     * @param includeInterim Include interim results
     * @param bucket The bucket to be expanded
     * @return The number of records added to the bucket
     */
    public int expandBucket(String jobId, boolean includeInterim, Bucket bucket) throws ResourceNotFoundException {
        int from = 0;

        QueryPage<AnomalyRecord> page = bucketRecords(
                jobId, bucket, from, RECORDS_SIZE_PARAM, includeInterim,
                AnomalyRecord.PROBABILITY.getPreferredName(), false, null);
        bucket.setRecords(page.results());

        while (page.count() > from + RECORDS_SIZE_PARAM) {
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
            throws ResourceNotFoundException {
        // Find the records using the time stamp rather than a parent-child
        // relationship.  The parent-child filter involves two queries behind
        // the scenes, and Elasticsearch documentation claims it's significantly
        // slower.  Here we rely on the record timestamps being identical to the
        // bucket timestamp.
        QueryBuilder recordFilter = QueryBuilders.termQuery(Bucket.TIMESTAMP.getPreferredName(), bucket.getTimestamp().getTime());

        recordFilter = new ResultsFilterBuilder(recordFilter)
                .interim(AnomalyRecord.IS_INTERIM.getPreferredName(), includeInterim)
                .term(AnomalyRecord.PARTITION_FIELD_VALUE.getPreferredName(), partitionFieldValue)
                .build();

        FieldSortBuilder sb = null;
        if (sortField != null) {
            sb = new FieldSortBuilder(sortField)
                    .missing("_last")
                    .order(descending ? SortOrder.DESC : SortOrder.ASC);
        }

        return records(jobId, from, size, recordFilter, sb, SECONDARY_SORT,
                descending);
    }

    /**
     * Get a page of {@linkplain CategoryDefinition}s for the given <code>jobId</code>.
     *
     * @param jobId the job id
     * @param from Skip the first N categories. This parameter is for paging
     * @param size Take only this number of categories
     * @return QueryPage of CategoryDefinition
     */
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
                throw new ElasticsearchParseException("failed to parse category definition", e);
            }
            CategoryDefinition categoryDefinition = CategoryDefinition.PARSER.apply(parser, () -> parseFieldMatcher);
            results.add(categoryDefinition);
        }

        return new QueryPage<>(results, searchResponse.getHits().getTotalHits(), CategoryDefinition.RESULTS_FIELD);
    }

    /**
     * Get the specific CategoryDefinition for the given job and category id.
     *
     * @param jobId the job id
     * @param categoryId Unique id
     * @return QueryPage CategoryDefinition
     */
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
                throw new ElasticsearchParseException("failed to parse category definition", e);
            }
            CategoryDefinition definition = CategoryDefinition.PARSER.apply(parser, () -> parseFieldMatcher);
            return new QueryPage<>(Collections.singletonList(definition), 1, CategoryDefinition.RESULTS_FIELD);
        }

        throw QueryPage.emptyQueryPage(Bucket.RESULTS_FIELD);
    }

    /**
     * Search for anomaly records with the parameters in the
     * {@link org.elasticsearch.xpack.prelert.job.persistence.RecordsQueryBuilder.RecordsQuery}
     * @return QueryPage of AnomalyRecords
     */
    public QueryPage<AnomalyRecord> records(String jobId, RecordsQueryBuilder.RecordsQuery query)
            throws ResourceNotFoundException {
        QueryBuilder fb = new ResultsFilterBuilder()
                .timeRange(Bucket.TIMESTAMP.getPreferredName(), query.getStart(), query.getEnd())
                .score(AnomalyRecord.ANOMALY_SCORE.getPreferredName(), query.getAnomalyScoreThreshold())
                .score(AnomalyRecord.NORMALIZED_PROBABILITY.getPreferredName(), query.getNormalizedProbabilityThreshold())
                .interim(AnomalyRecord.IS_INTERIM.getPreferredName(), query.isIncludeInterim())
                .term(AnomalyRecord.PARTITION_FIELD_VALUE.getPreferredName(), query.getPartitionFieldValue()).build();

        return records(jobId, query.getFrom(), query.getSize(), fb, query.getSortField(), query.isSortDescending());
    }


    private QueryPage<AnomalyRecord> records(String jobId,
                                             int from, int size, QueryBuilder recordFilter,
                                             String sortField, boolean descending)
            throws ResourceNotFoundException {
        FieldSortBuilder sb = null;
        if (sortField != null) {
            sb = new FieldSortBuilder(sortField)
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

        for (String sortField : secondarySort) {
            searchBuilder.addSort(sortField, descending ? SortOrder.DESC : SortOrder.ASC);
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
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            BytesReference source = hit.getSourceRef();
            XContentParser parser;
            try {
                parser = XContentFactory.xContent(source).createParser(source);
            } catch (IOException e) {
                throw new ElasticsearchParseException("failed to parse records", e);
            }

            results.add(AnomalyRecord.PARSER.apply(parser, () -> parseFieldMatcher));
        }

        return new QueryPage<>(results, searchResponse.getHits().getTotalHits(), AnomalyRecord.RESULTS_FIELD);
    }

    /**
     * Return a page of influencers for the given job and within the given date
     * range
     *
     * @param jobId
     *            The job ID for which influencers are requested
     * @param query
     *            the query
     * @return QueryPage of Influencer
     */
    public QueryPage<Influencer> influencers(String jobId, InfluencersQuery query) throws ResourceNotFoundException {
        QueryBuilder fb = new ResultsFilterBuilder()
                .timeRange(Bucket.TIMESTAMP.getPreferredName(), query.getStart(), query.getEnd())
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
                        " with sort " + (sortDescending ? "descending" : "ascending") + " on field " + sortField : "",
                () -> from, () -> size);

        filterBuilder = new BoolQueryBuilder()
                .filter(filterBuilder)
                .filter(new TermsQueryBuilder(Result.RESULT_TYPE.getPreferredName(), Influencer.RESULT_TYPE_VALUE));

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexName)
                .setTypes(Result.TYPE.getPreferredName())
                .setQuery(filterBuilder)
                .setFrom(from).setSize(size);

        FieldSortBuilder sb = sortField == null ? SortBuilders.fieldSort(ElasticsearchMappings.ES_DOC)
                : new FieldSortBuilder(sortField).order(sortDescending ? SortOrder.DESC : SortOrder.ASC);
        searchRequestBuilder.addSort(sb);

        SearchResponse response;
        try {
            response = searchRequestBuilder.get();
        } catch (IndexNotFoundException e) {
            throw ExceptionsHelper.missingJobException(jobId);
        }

        List<Influencer> influencers = new ArrayList<>();
        for (SearchHit hit : response.getHits().getHits()) {
            BytesReference source = hit.getSourceRef();
            XContentParser parser;
            try {
                parser = XContentFactory.xContent(source).createParser(source);
            } catch (IOException e) {
                throw new ElasticsearchParseException("failed to parse influencer", e);
            }

            influencers.add(Influencer.PARSER.apply(parser, () -> parseFieldMatcher));
        }

        return new QueryPage<>(influencers, response.getHits().getTotalHits(), Influencer.RESULTS_FIELD);
    }

    /**
     * Get the influencer for the given job for id
     *
     * @param jobId the job id
     * @param influencerId The unique influencer Id
     * @return Optional Influencer
     */
    public Optional<Influencer> influencer(String jobId, String influencerId) {
        throw new IllegalStateException();
    }

    /**
     * Returns a {@link BatchedDocumentsIterator} that allows querying
     * and iterating over a large number of influencers of the given job
     *
     * @param jobId the id of the job for which influencers are requested
     * @return an influencer {@link BatchedDocumentsIterator}
     */
    public BatchedDocumentsIterator<Influencer> newBatchedInfluencersIterator(String jobId) {
        return new ElasticsearchBatchedInfluencersIterator(client, jobId, parseFieldMatcher);
    }

    /**
     * Returns a {@link BatchedDocumentsIterator} that allows querying
     * and iterating over a number of model snapshots of the given job
     *
     * @param jobId the id of the job for which model snapshots are requested
     * @return a model snapshot {@link BatchedDocumentsIterator}
     */
    public BatchedDocumentsIterator<ModelSnapshot> newBatchedModelSnapshotIterator(String jobId) {
        return new ElasticsearchBatchedModelSnapshotIterator(client, jobId, parseFieldMatcher);
    }

    /**
     * Get the persisted quantiles state for the job
     */
    public Optional<Quantiles> getQuantiles(String jobId) {
        String indexName = JobResultsPersister.getJobIndexName(jobId);
        try {
            LOGGER.trace("ES API CALL: get ID " + Quantiles.QUANTILES_ID +
                    " type " + Quantiles.TYPE + " from index " + indexName);
            GetResponse response = client.prepareGet(
                    indexName, Quantiles.TYPE.getPreferredName(), Quantiles.QUANTILES_ID).get();
            if (!response.isExists()) {
                LOGGER.info("There are currently no quantiles for job " + jobId);
                return Optional.empty();
            }
            return Optional.of(createQuantiles(jobId, response));
        } catch (IndexNotFoundException e) {
            LOGGER.error("Missing index when getting quantiles", e);
            throw e;
        }
    }

    /**
     * Get model snapshots for the job ordered by descending restore priority.
     *
     * @param jobId the job id
     * @param from  number of snapshots to from
     * @param size  number of snapshots to retrieve
     * @return page of model snapshots
     */
    public QueryPage<ModelSnapshot> modelSnapshots(String jobId, int from, int size) {
        return modelSnapshots(jobId, from, size, null, null, null, true, null, null);
    }

    /**
     * Get model snapshots for the job ordered by descending restore priority.
     *
     * @param jobId          the job id
     * @param from           number of snapshots to from
     * @param size           number of snapshots to retrieve
     * @param startEpochMs   earliest time to include (inclusive)
     * @param endEpochMs     latest time to include (exclusive)
     * @param sortField      optional sort field name (may be null)
     * @param sortDescending Sort in descending order
     * @param snapshotId     optional snapshot ID to match (null for all)
     * @param description    optional description to match (null for all)
     * @return page of model snapshots
     */
    public QueryPage<ModelSnapshot> modelSnapshots(String jobId, int from, int size,
                                                   String startEpochMs, String endEpochMs, String sortField, boolean sortDescending,
                                                   String snapshotId, String description) {
        boolean haveId = snapshotId != null && !snapshotId.isEmpty();
        boolean haveDescription = description != null && !description.isEmpty();
        ResultsFilterBuilder fb;
        if (haveId || haveDescription) {
            BoolQueryBuilder query = QueryBuilders.boolQuery();
            if (haveId) {
                query.must(QueryBuilders.termQuery(ModelSnapshot.SNAPSHOT_ID.getPreferredName(), snapshotId));
            }
            if (haveDescription) {
                query.must(QueryBuilders.termQuery(ModelSnapshot.DESCRIPTION.getPreferredName(), description));
            }

            fb = new ResultsFilterBuilder(query);
        } else {
            fb = new ResultsFilterBuilder();
        }

        return modelSnapshots(jobId, from, size,
                (sortField == null || sortField.isEmpty()) ? ModelSnapshot.RESTORE_PRIORITY.getPreferredName() : sortField,
                sortDescending, fb.timeRange(
                        Bucket.TIMESTAMP.getPreferredName(), startEpochMs, endEpochMs).build());
    }

    private QueryPage<ModelSnapshot> modelSnapshots(String jobId, int from, int size,
                                                    String sortField, boolean sortDescending, QueryBuilder fb) {
        FieldSortBuilder sb = new FieldSortBuilder(sortField)
                .order(sortDescending ? SortOrder.DESC : SortOrder.ASC);

        // Wrap in a constant_score because we always want to
        // run it as a filter
        fb = new ConstantScoreQueryBuilder(fb);

        SearchResponse searchResponse;
        try {
            String indexName = JobResultsPersister.getJobIndexName(jobId);
            LOGGER.trace("ES API CALL: search all of type {} from index {} sort ascending {} with filter after sort from {} size {}",
                ModelSnapshot.TYPE, indexName, sortField, from, size);

            searchResponse = client.prepareSearch(indexName)
                    .setTypes(ModelSnapshot.TYPE.getPreferredName())
                    .addSort(sb)
                    .setQuery(fb)
                    .setFrom(from).setSize(size)
                    .get();
        } catch (IndexNotFoundException e) {
            LOGGER.error("Failed to read modelSnapshots", e);
            throw e;
        }

        List<ModelSnapshot> results = new ArrayList<>();

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            BytesReference source = hit.getSourceRef();
            XContentParser parser;
            try {
                parser = XContentFactory.xContent(source).createParser(source);
            } catch (IOException e) {
                throw new ElasticsearchParseException("failed to parse modelSnapshot", e);
            }
            ModelSnapshot modelSnapshot = ModelSnapshot.PARSER.apply(parser, () -> parseFieldMatcher);
            results.add(modelSnapshot);
        }

        return new QueryPage<>(results, searchResponse.getHits().getTotalHits(), ModelSnapshot.RESULTS_FIELD);
    }

    /**
     * Given a model snapshot, get the corresponding state and write it to the supplied
     * stream.  If there are multiple state documents they are separated using <code>'\0'</code>
     * when written to the stream.
     * @param jobId         the job id
     * @param modelSnapshot the model snapshot to be restored
     * @param restoreStream the stream to write the state to
     */
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

            GetResponse stateResponse = client.prepareGet(indexName, ModelState.TYPE.getPreferredName(), docId).get();
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
            throw new ElasticsearchParseException("failed to parse quantiles", e);
        }
        Quantiles quantiles = Quantiles.PARSER.apply(parser, () -> parseFieldMatcher);
        if (quantiles.getQuantileState() == null) {
            LOGGER.error("Inconsistency - no " + Quantiles.QUANTILE_STATE
                    + " field in quantiles for job " + jobId);
        }
        return quantiles;
    }

    public QueryPage<ModelDebugOutput> modelDebugOutput(String jobId, int from, int size) {

        SearchResponse searchResponse;
        try {
            String indexName = JobResultsPersister.getJobIndexName(jobId);
            LOGGER.trace("ES API CALL: search result type {} from index {} from {}, size {]",
                    ModelDebugOutput.RESULT_TYPE_VALUE, indexName, from, size);

            searchResponse = client.prepareSearch(indexName)
                    .setTypes(Result.TYPE.getPreferredName())
                    .setQuery(new TermsQueryBuilder(Result.RESULT_TYPE.getPreferredName(), ModelDebugOutput.RESULT_TYPE_VALUE))
                    .setFrom(from).setSize(size)
                    .get();
        } catch (IndexNotFoundException e) {
            throw ExceptionsHelper.missingJobException(jobId);
        }

        List<ModelDebugOutput> results = new ArrayList<>();

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            BytesReference source = hit.getSourceRef();
            XContentParser parser;
            try {
                parser = XContentFactory.xContent(source).createParser(source);
            } catch (IOException e) {
                throw new ElasticsearchParseException("failed to parse modelDebugOutput", e);
            }
            ModelDebugOutput modelDebugOutput = ModelDebugOutput.PARSER.apply(parser, () -> parseFieldMatcher);
            results.add(modelDebugOutput);
        }

        return new QueryPage<>(results, searchResponse.getHits().getTotalHits(), ModelDebugOutput.RESULTS_FIELD);
    }

    /**
     * Get the job's model size stats.
     */
    public Optional<ModelSizeStats> modelSizeStats(String jobId) {
        String indexName = JobResultsPersister.getJobIndexName(jobId);
        try {
            LOGGER.trace("ES API CALL: get result type {} ID {} from index {}",
                    ModelSizeStats.RESULT_TYPE_VALUE, ModelSizeStats.RESULT_TYPE_FIELD, indexName);

            GetResponse modelSizeStatsResponse = client.prepareGet(
                    indexName, Result.TYPE.getPreferredName(), ModelSizeStats.RESULT_TYPE_FIELD.getPreferredName()).get();

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
                    throw new ElasticsearchParseException("failed to parse model size stats", e);
                }
                ModelSizeStats modelSizeStats = ModelSizeStats.PARSER.apply(parser, () -> parseFieldMatcher).build();
                return Optional.of(modelSizeStats);
            }
        } catch (IndexNotFoundException e) {
            LOGGER.warn("Missing index " + indexName, e);
            return Optional.empty();
        }
    }

    /**
     * Retrieves the list with the given {@code listId} from the datastore.
     *
     * @param listId the id of the requested list
     * @return the matching list if it exists
     */
    public Optional<ListDocument> getList(String listId) {
        GetResponse response = client.prepareGet(PRELERT_INFO_INDEX, ListDocument.TYPE.getPreferredName(), listId).get();
        if (!response.isExists()) {
            return Optional.empty();
        }
        BytesReference source = response.getSourceAsBytesRef();
        XContentParser parser;
        try {
            parser = XContentFactory.xContent(source).createParser(source);
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to parse list", e);
        }
        ListDocument listDocument = ListDocument.PARSER.apply(parser, () -> parseFieldMatcher);
        return Optional.of(listDocument);
    }

    /**
     * Get an auditor for the given job
     *
     * @param jobId the job id
     * @return the {@code Auditor}
     */
    public Auditor audit(String jobId) {
         return new Auditor(client, JobResultsPersister.getJobIndexName(jobId), jobId);
    }
}
