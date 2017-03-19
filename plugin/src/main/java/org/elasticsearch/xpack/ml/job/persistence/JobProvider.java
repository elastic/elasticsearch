/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.UidFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.ml.action.GetCategoriesAction;
import org.elasticsearch.xpack.ml.action.GetInfluencersAction;
import org.elasticsearch.xpack.ml.action.GetRecordsAction;
import org.elasticsearch.xpack.ml.action.util.QueryPage;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.MlFilter;
import org.elasticsearch.xpack.ml.job.persistence.BucketsQueryBuilder.BucketsQuery;
import org.elasticsearch.xpack.ml.job.persistence.InfluencersQueryBuilder.InfluencersQuery;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.CategorizerState;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelState;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.ml.job.results.Bucket;
import org.elasticsearch.xpack.ml.job.results.CategoryDefinition;
import org.elasticsearch.xpack.ml.job.results.Influencer;
import org.elasticsearch.xpack.ml.job.results.ModelPlot;
import org.elasticsearch.xpack.ml.job.results.PerPartitionMaxProbabilities;
import org.elasticsearch.xpack.ml.job.results.Result;
import org.elasticsearch.xpack.security.support.Exceptions;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.ml.job.persistence.AnomalyDetectorsIndex.ML_META_INDEX;

public class JobProvider {
    private static final Logger LOGGER = Loggers.getLogger(JobProvider.class);

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
    private final Settings settings;

    public JobProvider(Client client, Settings settings) {
        this.client = Objects.requireNonNull(client);
        this.settings = settings;
    }

    /**
     * Create the Elasticsearch index and the mappings
     */
    public void createJobResultIndex(Job job, ClusterState state, final ActionListener<Boolean> finalListener) {
        Collection<String> termFields = (job.getAnalysisConfig() != null) ? job.getAnalysisConfig().termFields() : Collections.emptyList();

        String aliasName = AnomalyDetectorsIndex.jobResultsAliasedName(job.getId());
        String indexName = job.getResultsIndexName();

        final ActionListener<Boolean> createAliasListener = ActionListener.wrap(success -> {
                    client.admin().indices().prepareAliases()
                            .addAlias(indexName, aliasName, QueryBuilders.termQuery(Job.ID.getPreferredName(), job.getId()))
                            // we could return 'sucess && r.isAcknowledged()' instead of 'true', but that makes
                            // testing not possible as we can't create IndicesAliasesResponse instance or
                            // mock IndicesAliasesResponse#isAcknowledged()
                            .execute(ActionListener.wrap(r -> finalListener.onResponse(true),
                                    finalListener::onFailure));
                },
                finalListener::onFailure);

        // Indices can be shared, so only create if it doesn't exist already. Saves us a roundtrip if
        // already in the CS
        if (!state.getMetaData().hasIndex(indexName)) {
            LOGGER.trace("ES API CALL: create index {}", indexName);
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
            String type = Result.TYPE.getPreferredName();
            createIndexRequest.mapping(type, ElasticsearchMappings.termFieldsMapping(type, termFields));
            client.admin().indices().create(createIndexRequest,
                    ActionListener.wrap(
                            r -> createAliasListener.onResponse(r.isAcknowledged()),
                            e -> {
                                // Possible that the index was created while the request was executing,
                                // so we need to handle that possibility
                                if (e instanceof ResourceAlreadyExistsException) {
                                    LOGGER.info("Index already exists");
                                    // Create the alias
                                    createAliasListener.onResponse(true);
                                } else {
                                    finalListener.onFailure(e);
                                }
                            }
                    ));
        } else {
            long fieldCountLimit = MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.get(settings);
            if (violatedFieldCountLimit(indexName, termFields.size(), fieldCountLimit, state)) {
                String message = "Cannot create job in index '" + indexName + "' as the " +
                        MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey() + " setting will be violated";
                finalListener.onFailure(new IllegalArgumentException(message));
            } else {
                updateIndexMappingWithTermFields(indexName, termFields,
                        ActionListener.wrap(createAliasListener::onResponse, finalListener::onFailure));
            }
        }
    }

    static boolean violatedFieldCountLimit(String indexName, long additionalFieldCount, long fieldCountLimit, ClusterState clusterState) {
        long numFields = 0;
        IndexMetaData indexMetaData = clusterState.metaData().index(indexName);
        Iterator<MappingMetaData> mappings = indexMetaData.getMappings().valuesIt();
        while (mappings.hasNext()) {
            MappingMetaData mapping = mappings.next();
            try {
                numFields += countFields(mapping.sourceAsMap());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        if (numFields + additionalFieldCount > fieldCountLimit) {
            return true;
        } else {
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    static int countFields(Map<String, Object> mapping) {
        Object propertiesNode = mapping.get("properties");
        if (propertiesNode != null && propertiesNode instanceof Map) {
            mapping = (Map<String, Object>) propertiesNode;
        } else {
            return 0;
        }

        int count = 0;
        for (Map.Entry<String, Object> entry : mapping.entrySet()) {
            if (entry.getValue() instanceof Map) {
                Map<String, Object> fieldMapping = (Map<String, Object>) entry.getValue();
                // take into account object and nested fields:
                count += countFields(fieldMapping);
            }
            count++;
        }
        return count;
    }

    private void updateIndexMappingWithTermFields(String indexName, Collection<String> termFields, ActionListener<Boolean> listener) {
        client.admin().indices().preparePutMapping(indexName).setType(Result.TYPE.getPreferredName())
                .setSource(ElasticsearchMappings.termFieldsMapping(null, termFields))
                .execute(new ActionListener<PutMappingResponse>() {
                    @Override
                    public void onResponse(PutMappingResponse putMappingResponse) {
                        listener.onResponse(putMappingResponse.isAcknowledged());
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }
                });
    }

    /**
     * Get the job's data counts
     *
     * @param jobId The job id
     */
    public void dataCounts(String jobId, Consumer<DataCounts> handler, Consumer<Exception> errorHandler) {
        String indexName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
        get(jobId, indexName, DataCounts.TYPE.getPreferredName(), DataCounts.documentId(jobId), handler, errorHandler,
                DataCounts.PARSER, () -> new DataCounts(jobId));
    }

    private <T, U> void get(String jobId, String indexName, String type, String id, Consumer<T> handler, Consumer<Exception> errorHandler,
                            BiFunction<XContentParser, U, T> objectParser, Supplier<T> notFoundSupplier) {
        GetRequest getRequest = new GetRequest(indexName, type, id);
        client.get(getRequest, ActionListener.wrap(
                response -> {
                    if (response.isExists() == false) {
                        handler.accept(notFoundSupplier.get());
                    } else {
                        BytesReference source = response.getSourceAsBytesRef();
                        try (XContentParser parser = XContentFactory.xContent(source).createParser(NamedXContentRegistry.EMPTY, source)) {
                            handler.accept(objectParser.apply(parser, null));
                        } catch (IOException e) {
                            throw new ElasticsearchParseException("failed to parse " + type, e);
                        }
                    }
                },
                error -> {
                    if (error instanceof IndexNotFoundException == false) {
                        errorHandler.accept(error);
                    } else {
                        handler.accept(notFoundSupplier.get());
                    }
                }));
    }

    private <T, U> void mget(String indexName, String type, Set<String> ids, Consumer<Set<T>> handler, Consumer<Exception> errorHandler,
                             BiFunction<XContentParser, U, T> objectParser) {
        if (ids.isEmpty()) {
            handler.accept(Collections.emptySet());
            return;
        }

        MultiGetRequest multiGetRequest = new MultiGetRequest();
        for (String id : ids) {
            multiGetRequest.add(indexName, type, id);
        }
        client.multiGet(multiGetRequest, ActionListener.wrap(
                mresponse -> {
                    Set<T> objects = new HashSet<>();
                    for (MultiGetItemResponse item : mresponse) {
                        GetResponse response = item.getResponse();
                        if (response.isExists()) {
                            BytesReference source = response.getSourceAsBytesRef();
                            try (XContentParser parser = XContentFactory.xContent(source)
                                    .createParser(NamedXContentRegistry.EMPTY, source)) {
                                objects.add(objectParser.apply(parser, null));
                            } catch (IOException e) {
                                throw new ElasticsearchParseException("failed to parse " + type, e);
                            }
                        }
                    }
                    handler.accept(objects);
                },
                e -> {
                    if (e instanceof IndexNotFoundException == false) {
                        errorHandler.accept(e);
                    } else {
                        handler.accept(new HashSet<>());
                    }
                })
        );
    }

    public static IndicesOptions addIgnoreUnavailable(IndicesOptions indicesOptions) {
        return IndicesOptions.fromOptions(true, indicesOptions.allowNoIndices(), indicesOptions.expandWildcardsOpen(),
                indicesOptions.expandWildcardsClosed(), indicesOptions);
    }

    /**
     * Search for buckets with the parameters in the {@link BucketsQueryBuilder}
     * Uses the internal client, so runs as the _xpack user
     */
    public void bucketsViaInternalClient(String jobId, BucketsQuery query, Consumer<QueryPage<Bucket>> handler,
                                         Consumer<Exception> errorHandler)
            throws ResourceNotFoundException {
        buckets(jobId, query, handler, errorHandler, client);
    }

    /**
     * Search for buckets with the parameters in the {@link BucketsQueryBuilder}
     * Uses a supplied client, so may run as the currently authenticated user
     */
    public void buckets(String jobId, BucketsQuery query, Consumer<QueryPage<Bucket>> handler, Consumer<Exception> errorHandler,
                        Client client) throws ResourceNotFoundException {

        ResultsFilterBuilder rfb = new ResultsFilterBuilder();
        if (query.getTimestamp() != null) {
            rfb.timeRange(Result.TIMESTAMP.getPreferredName(), query.getTimestamp());
        } else {
            rfb.timeRange(Result.TIMESTAMP.getPreferredName(), query.getStart(), query.getEnd())
                    .score(Bucket.ANOMALY_SCORE.getPreferredName(), query.getAnomalyScoreFilter())
                    .score(Bucket.MAX_NORMALIZED_PROBABILITY.getPreferredName(), query.getNormalizedProbability())
                    .interim(Bucket.IS_INTERIM.getPreferredName(), query.isIncludeInterim());
        }

        SortBuilder<?> sortBuilder = new FieldSortBuilder(query.getSortField())
                .order(query.isSortDescending() ? SortOrder.DESC : SortOrder.ASC);

        QueryBuilder boolQuery = new BoolQueryBuilder()
                .filter(rfb.build())
                .filter(QueryBuilders.termQuery(Result.RESULT_TYPE.getPreferredName(), Bucket.RESULT_TYPE_VALUE));
        String indexName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.types(Result.TYPE.getPreferredName());
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.sort(sortBuilder);
        searchSourceBuilder.query(boolQuery);
        searchSourceBuilder.from(query.getFrom());
        searchSourceBuilder.size(query.getSize());
        searchRequest.source(searchSourceBuilder);

        MultiSearchRequest mrequest = new MultiSearchRequest();
        mrequest.indicesOptions(addIgnoreUnavailable(mrequest.indicesOptions()));
        mrequest.add(searchRequest);
        if (Strings.hasLength(query.getPartitionValue())) {
            mrequest.add(createPartitionMaxNormailizedProbabilitiesRequest(jobId, query.getStart(), query.getEnd(),
                    query.getPartitionValue()));
        }

        client.multiSearch(mrequest, ActionListener.wrap(mresponse -> {
            MultiSearchResponse.Item item1 = mresponse.getResponses()[0];
            if (item1.isFailure()) {
                errorHandler.accept(mapAuthFailure(item1.getFailure(), jobId, GetBucketsAction.NAME));
                return;
            }

            SearchResponse searchResponse = item1.getResponse();
            SearchHits hits = searchResponse.getHits();
            if (query.getTimestamp() != null) {
                if (hits.getTotalHits() == 0) {
                    throw QueryPage.emptyQueryPage(Bucket.RESULTS_FIELD);
                } else if (hits.getTotalHits() > 1) {
                    LOGGER.error("Found more than one bucket with timestamp [{}] from index {}", query.getTimestamp(), indexName);
                }
            }

            List<Bucket> results = new ArrayList<>();
            for (SearchHit hit : hits.getHits()) {
                BytesReference source = hit.getSourceRef();
                try (XContentParser parser = XContentFactory.xContent(source).createParser(NamedXContentRegistry.EMPTY, source)) {
                    Bucket bucket = Bucket.PARSER.apply(parser, null);
                    if (query.isIncludeInterim() || bucket.isInterim() == false) {
                        results.add(bucket);
                    }
                } catch (IOException e) {
                    throw new ElasticsearchParseException("failed to parse bucket", e);
                }
            }

            if (query.getTimestamp() != null && results.isEmpty()) {
                throw QueryPage.emptyQueryPage(Bucket.RESULTS_FIELD);
            }

            QueryPage<Bucket> buckets = new QueryPage<>(results, searchResponse.getHits().getTotalHits(), Bucket.RESULTS_FIELD);
            if (Strings.hasLength(query.getPartitionValue())) {
                MultiSearchResponse.Item item2 = mresponse.getResponses()[1];
                if (item2.isFailure()) {
                    errorHandler.accept(item2.getFailure());
                    return;
                }
                List<PerPartitionMaxProbabilities> partitionProbs =
                        handlePartitionMaxNormailizedProbabilitiesResponse(item2.getResponse());
                mergePartitionScoresIntoBucket(partitionProbs, buckets.results(), query.getPartitionValue());

                if (query.isExpand()) {
                    Iterator<Bucket> bucketsToExpand = buckets.results().stream()
                            .filter(bucket -> bucket.getRecordCount() > 0).iterator();
                    expandBuckets(jobId, query, buckets, bucketsToExpand, 0, handler, errorHandler, client);
                    return;
                }
            } else {
                if (query.isExpand()) {
                    Iterator<Bucket> bucketsToExpand = buckets.results().stream()
                            .filter(bucket -> bucket.getRecordCount() > 0).iterator();
                    expandBuckets(jobId, query, buckets, bucketsToExpand, 0, handler, errorHandler, client);
                    return;
                }
            }
            handler.accept(buckets);
        }, errorHandler));
    }

    private void expandBuckets(String jobId, BucketsQuery query, QueryPage<Bucket> buckets, Iterator<Bucket> bucketsToExpand,
                               int from, Consumer<QueryPage<Bucket>> handler, Consumer<Exception> errorHandler, Client client) {
        if (bucketsToExpand.hasNext()) {
            Consumer<Integer> c = i -> {
                expandBuckets(jobId, query, buckets, bucketsToExpand, from + RECORDS_SIZE_PARAM, handler, errorHandler, client);
            };
            expandBucket(jobId, query.isIncludeInterim(), bucketsToExpand.next(), query.getPartitionValue(), from, c, errorHandler, client);
        } else {
            handler.accept(buckets);
        }
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

    private SearchRequest createPartitionMaxNormailizedProbabilitiesRequest(String jobId, Object epochStart, Object epochEnd,
                                                                            String partitionFieldValue) {
        QueryBuilder timeRangeQuery = new ResultsFilterBuilder()
                .timeRange(Result.TIMESTAMP.getPreferredName(), epochStart, epochEnd)
                .build();

        QueryBuilder boolQuery = new BoolQueryBuilder()
                .filter(timeRangeQuery)
                .filter(new TermsQueryBuilder(Result.RESULT_TYPE.getPreferredName(), PerPartitionMaxProbabilities.RESULT_TYPE_VALUE))
                .filter(new TermsQueryBuilder(AnomalyRecord.PARTITION_FIELD_VALUE.getPreferredName(), partitionFieldValue));

        FieldSortBuilder sb = new FieldSortBuilder(Result.TIMESTAMP.getPreferredName()).order(SortOrder.ASC);
        String indexName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.sort(sb);
        sourceBuilder.query(boolQuery);
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(sourceBuilder);
        searchRequest.indicesOptions(addIgnoreUnavailable(searchRequest.indicesOptions()));
        return searchRequest;
    }

    private List<PerPartitionMaxProbabilities> handlePartitionMaxNormailizedProbabilitiesResponse(SearchResponse searchResponse) {
        List<PerPartitionMaxProbabilities> results = new ArrayList<>();
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            BytesReference source = hit.getSourceRef();
            try (XContentParser parser = XContentFactory.xContent(source).createParser(NamedXContentRegistry.EMPTY, source)) {
                results.add(PerPartitionMaxProbabilities.PARSER.apply(parser, null));
            } catch (IOException e) {
                throw new ElasticsearchParseException("failed to parse PerPartitionMaxProbabilities", e);
            }
        }
        return results;
    }

    /**
     * Returns a {@link BatchedDocumentsIterator} that allows querying
     * and iterating over a large number of buckets of the given job.
     * The bucket and source indexes are returned by the iterator.
     *
     * @param jobId the id of the job for which buckets are requested
     * @return a bucket {@link BatchedDocumentsIterator}
     */
    public BatchedDocumentsIterator<BatchedResultsIterator.ResultWithIndex<Bucket>> newBatchedBucketsIterator(String jobId) {
        return new BatchedBucketsIterator(client, jobId);
    }

    /**
     * Returns a {@link BatchedDocumentsIterator} that allows querying
     * and iterating over a large number of records in the given job
     * The records and source indexes are returned by the iterator.
     *
     * @param jobId the id of the job for which buckets are requested
     * @return a record {@link BatchedDocumentsIterator}
     */
    public BatchedDocumentsIterator<BatchedResultsIterator.ResultWithIndex<AnomalyRecord>>
    newBatchedRecordsIterator(String jobId) {
        return new BatchedRecordsIterator(client, jobId);
    }

    /**
     * Expand a bucket with its records
     */
    // TODO (norelease): Use scroll search instead of multiple searches with increasing from
    public void expandBucket(String jobId, boolean includeInterim, Bucket bucket, String partitionFieldValue, int from,
                             Consumer<Integer> consumer, Consumer<Exception> errorHandler, Client client) {
        Consumer<QueryPage<AnomalyRecord>> h = page -> {
            bucket.getRecords().addAll(page.results());
            if (partitionFieldValue != null) {
                bucket.setAnomalyScore(bucket.partitionAnomalyScore(partitionFieldValue));
            }
            if (page.count() > from + RECORDS_SIZE_PARAM) {
                expandBucket(jobId, includeInterim, bucket, partitionFieldValue, from + RECORDS_SIZE_PARAM, consumer, errorHandler,
                        client);
            } else {
                consumer.accept(bucket.getRecords().size());
            }
        };
        bucketRecords(jobId, bucket, from, RECORDS_SIZE_PARAM, includeInterim, AnomalyRecord.PROBABILITY.getPreferredName(),
                false, partitionFieldValue, h, errorHandler, client);
    }

    void bucketRecords(String jobId, Bucket bucket, int from, int size, boolean includeInterim, String sortField,
                       boolean descending, String partitionFieldValue, Consumer<QueryPage<AnomalyRecord>> handler,
                       Consumer<Exception> errorHandler, Client client) {
        // Find the records using the time stamp rather than a parent-child
        // relationship.  The parent-child filter involves two queries behind
        // the scenes, and Elasticsearch documentation claims it's significantly
        // slower.  Here we rely on the record timestamps being identical to the
        // bucket timestamp.
        QueryBuilder recordFilter = QueryBuilders.termQuery(Result.TIMESTAMP.getPreferredName(), bucket.getTimestamp().getTime());

        ResultsFilterBuilder builder = new ResultsFilterBuilder(recordFilter)
                .interim(AnomalyRecord.IS_INTERIM.getPreferredName(), includeInterim);
        if (partitionFieldValue != null) {
            builder.term(AnomalyRecord.PARTITION_FIELD_VALUE.getPreferredName(), partitionFieldValue);
        }
        recordFilter = builder.build();

        FieldSortBuilder sb = null;
        if (sortField != null) {
            sb = new FieldSortBuilder(sortField)
                    .missing("_last")
                    .order(descending ? SortOrder.DESC : SortOrder.ASC);
        }

        records(jobId, from, size, recordFilter, sb, SECONDARY_SORT, descending, handler, errorHandler, client);
    }

    /**
     * Get a page of {@linkplain CategoryDefinition}s for the given <code>jobId</code>.
     * Uses the internal client, so runs as the _xpack user
     * @param jobId the job id
     * @param from  Skip the first N categories. This parameter is for paging
     * @param size  Take only this number of categories
     */
    public void categoryDefinitionsViaInternalClient(String jobId, String categoryId, Integer from, Integer size,
                                                     Consumer<QueryPage<CategoryDefinition>> handler,
                                                     Consumer<Exception> errorHandler) {
        categoryDefinitions(jobId, categoryId, from, size, handler, errorHandler, client);
    }

    /**
     * Get a page of {@linkplain CategoryDefinition}s for the given <code>jobId</code>.
     * Uses a supplied client, so may run as the currently authenticated user
     * @param jobId the job id
     * @param from  Skip the first N categories. This parameter is for paging
     * @param size  Take only this number of categories
     */
    public void categoryDefinitions(String jobId, String categoryId, Integer from, Integer size,
                                    Consumer<QueryPage<CategoryDefinition>> handler,
                                    Consumer<Exception> errorHandler, Client client) {
        if (categoryId != null && (from != null || size != null)) {
            throw new IllegalStateException("Both categoryId and pageParams are specified");
        }

        String indexName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
        LOGGER.trace("ES API CALL: search all of type {} from index {} sort ascending {} from {} size {}",
                CategoryDefinition.TYPE.getPreferredName(), indexName, CategoryDefinition.CATEGORY_ID.getPreferredName(), from, size);

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.indicesOptions(addIgnoreUnavailable(searchRequest.indicesOptions()));
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        if (categoryId != null) {
            String documentId = CategoryDefinition.documentId(jobId, categoryId);
            String uid = Uid.createUid(CategoryDefinition.TYPE.getPreferredName(), documentId);
            sourceBuilder.query(QueryBuilders.termQuery(UidFieldMapper.NAME, uid));
            searchRequest.routing(documentId);
        } else if (from != null && size != null) {
            searchRequest.types(CategoryDefinition.TYPE.getPreferredName());
            sourceBuilder.from(from).size(size)
                    .sort(new FieldSortBuilder(CategoryDefinition.CATEGORY_ID.getPreferredName()).order(SortOrder.ASC));
        } else {
            throw new IllegalStateException("Both categoryId and pageParams are not specified");
        }
        searchRequest.source(sourceBuilder);
        client.search(searchRequest, ActionListener.wrap(searchResponse -> {
            SearchHit[] hits = searchResponse.getHits().getHits();
            List<CategoryDefinition> results = new ArrayList<>(hits.length);
            for (SearchHit hit : hits) {
                BytesReference source = hit.getSourceRef();
                try (XContentParser parser = XContentFactory.xContent(source).createParser(NamedXContentRegistry.EMPTY, source)) {
                    CategoryDefinition categoryDefinition = CategoryDefinition.PARSER.apply(parser, null);
                    results.add(categoryDefinition);
                } catch (IOException e) {
                    throw new ElasticsearchParseException("failed to parse category definition", e);
                }
            }
            QueryPage<CategoryDefinition> result =
                    new QueryPage<>(results, searchResponse.getHits().getTotalHits(), CategoryDefinition.RESULTS_FIELD);
            handler.accept(result);
        }, e -> errorHandler.accept(mapAuthFailure(e, jobId, GetCategoriesAction.NAME))));
    }

    /**
     * Search for anomaly records with the parameters in the
     * {@link org.elasticsearch.xpack.ml.job.persistence.RecordsQueryBuilder.RecordsQuery}
     * Uses the internal client, so runs as the _xpack user
     */
    public void recordsViaInternalClient(String jobId, RecordsQueryBuilder.RecordsQuery query, Consumer<QueryPage<AnomalyRecord>> handler,
                                         Consumer<Exception> errorHandler) {
        records(jobId, query, handler, errorHandler, client);
    }

    /**
     * Search for anomaly records with the parameters in the
     * {@link org.elasticsearch.xpack.ml.job.persistence.RecordsQueryBuilder.RecordsQuery}
     * Uses a supplied client, so may run as the currently authenticated user
     */
    public void records(String jobId, RecordsQueryBuilder.RecordsQuery query, Consumer<QueryPage<AnomalyRecord>> handler,
                        Consumer<Exception> errorHandler, Client client) {
        QueryBuilder fb = new ResultsFilterBuilder()
                .timeRange(Result.TIMESTAMP.getPreferredName(), query.getStart(), query.getEnd())
                .score(AnomalyRecord.ANOMALY_SCORE.getPreferredName(), query.getAnomalyScoreThreshold())
                .score(AnomalyRecord.NORMALIZED_PROBABILITY.getPreferredName(), query.getNormalizedProbabilityThreshold())
                .interim(AnomalyRecord.IS_INTERIM.getPreferredName(), query.isIncludeInterim())
                .term(AnomalyRecord.PARTITION_FIELD_VALUE.getPreferredName(), query.getPartitionFieldValue()).build();
        FieldSortBuilder sb = null;
        if (query.getSortField() != null) {
            sb = new FieldSortBuilder(query.getSortField())
                    .missing("_last")
                    .order(query.isSortDescending() ? SortOrder.DESC : SortOrder.ASC);
        }
        records(jobId, query.getFrom(), query.getSize(), fb, sb, SECONDARY_SORT, query.isSortDescending(), handler, errorHandler, client);
    }

    /**
     * The returned records have their id set.
     */
    private void records(String jobId, int from, int size,
                         QueryBuilder recordFilter, FieldSortBuilder sb, List<String> secondarySort,
                         boolean descending, Consumer<QueryPage<AnomalyRecord>> handler,
                         Consumer<Exception> errorHandler, Client client) {
        String indexName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);

        recordFilter = new BoolQueryBuilder()
                .filter(recordFilter)
                .filter(new TermsQueryBuilder(Result.RESULT_TYPE.getPreferredName(), AnomalyRecord.RESULT_TYPE_VALUE));

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.indicesOptions(addIgnoreUnavailable(searchRequest.indicesOptions()));
        searchRequest.types(Result.TYPE.getPreferredName());
        searchRequest.source(new SearchSourceBuilder()
                .from(from)
                .size(size)
                .query(recordFilter)
                .sort(sb == null ? SortBuilders.fieldSort(ElasticsearchMappings.ES_DOC) : sb)
                .fetchSource(true)
        );

        for (String sortField : secondarySort) {
            searchRequest.source().sort(sortField, descending ? SortOrder.DESC : SortOrder.ASC);
        }

        LOGGER.trace("ES API CALL: search all of result type {} from index {}{}{}  with filter after sort from {} size {}",
                AnomalyRecord.RESULT_TYPE_VALUE, indexName, (sb != null) ? " with sort" : "",
                secondarySort.isEmpty() ? "" : " with secondary sort", from, size);
        client.search(searchRequest, ActionListener.wrap(searchResponse -> {
            List<AnomalyRecord> results = new ArrayList<>();
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                BytesReference source = hit.getSourceRef();
                try (XContentParser parser = XContentFactory.xContent(source).createParser(NamedXContentRegistry.EMPTY, source)) {
                    results.add(AnomalyRecord.PARSER.apply(parser, null));
                } catch (IOException e) {
                    throw new ElasticsearchParseException("failed to parse records", e);
                }
            }
            QueryPage<AnomalyRecord> queryPage =
                    new QueryPage<>(results, searchResponse.getHits().getTotalHits(), AnomalyRecord.RESULTS_FIELD);
            handler.accept(queryPage);
        }, e -> errorHandler.accept(mapAuthFailure(e, jobId, GetRecordsAction.NAME))));
    }

    /**
     * Return a page of influencers for the given job and within the given date range
     * Uses the internal client, so runs as the _xpack user
     * @param jobId The job ID for which influencers are requested
     * @param query the query
     */
    public void influencersViaInternalClient(String jobId, InfluencersQuery query, Consumer<QueryPage<Influencer>> handler,
                                             Consumer<Exception> errorHandler) {
        influencers(jobId, query, handler, errorHandler, client);
    }

    /**
     * Return a page of influencers for the given job and within the given date range
     * Uses a supplied client, so may run as the currently authenticated user
     * @param jobId The job ID for which influencers are requested
     * @param query the query
     */
    public void influencers(String jobId, InfluencersQuery query, Consumer<QueryPage<Influencer>> handler,
                            Consumer<Exception> errorHandler, Client client) {
        QueryBuilder fb = new ResultsFilterBuilder()
                .timeRange(Result.TIMESTAMP.getPreferredName(), query.getStart(), query.getEnd())
                .score(Bucket.ANOMALY_SCORE.getPreferredName(), query.getAnomalyScoreFilter())
                .interim(Bucket.IS_INTERIM.getPreferredName(), query.isIncludeInterim())
                .build();

        String indexName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
        LOGGER.trace("ES API CALL: search all of result type {} from index {}{}  with filter from {} size {}",
                () -> Influencer.RESULT_TYPE_VALUE, () -> indexName,
                () -> (query.getSortField() != null) ?
                        " with sort " + (query.isSortDescending() ? "descending" : "ascending") + " on field " + query.getSortField() : "",
                query::getFrom, query::getSize);

        QueryBuilder qb = new BoolQueryBuilder()
                .filter(fb)
                .filter(new TermsQueryBuilder(Result.RESULT_TYPE.getPreferredName(), Influencer.RESULT_TYPE_VALUE));

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.indicesOptions(addIgnoreUnavailable(searchRequest.indicesOptions()));
        searchRequest.types(Result.TYPE.getPreferredName());
        FieldSortBuilder sb = query.getSortField() == null ? SortBuilders.fieldSort(ElasticsearchMappings.ES_DOC)
                : new FieldSortBuilder(query.getSortField()).order(query.isSortDescending() ? SortOrder.DESC : SortOrder.ASC);
        searchRequest.source(new SearchSourceBuilder().query(qb).from(query.getFrom()).size(query.getSize()).sort(sb));

        client.search(searchRequest, ActionListener.wrap(response -> {
            List<Influencer> influencers = new ArrayList<>();
            for (SearchHit hit : response.getHits().getHits()) {
                BytesReference source = hit.getSourceRef();
                try (XContentParser parser = XContentFactory.xContent(source).createParser(NamedXContentRegistry.EMPTY, source)) {
                    influencers.add(Influencer.PARSER.apply(parser, null));
                } catch (IOException e) {
                    throw new ElasticsearchParseException("failed to parse influencer", e);
                }
            }
            QueryPage<Influencer> result = new QueryPage<>(influencers, response.getHits().getTotalHits(), Influencer.RESULTS_FIELD);
            handler.accept(result);
        }, e -> errorHandler.accept(mapAuthFailure(e, jobId, GetInfluencersAction.NAME))));
    }

    /**
     * Returns a {@link BatchedDocumentsIterator} that allows querying
     * and iterating over a large number of influencers of the given job
     *
     * @param jobId the id of the job for which influencers are requested
     * @return an influencer {@link BatchedDocumentsIterator}
     */
    public BatchedDocumentsIterator<BatchedResultsIterator.ResultWithIndex<Influencer>>
    newBatchedInfluencersIterator(String jobId) {
        return new BatchedInfluencersIterator(client, jobId);
    }

    /**
     * Get the persisted quantiles state for the job
     */
    public void getQuantiles(String jobId, Consumer<Quantiles> handler, Consumer<Exception> errorHandler) {
        String indexName = AnomalyDetectorsIndex.jobStateIndexName();
        String quantilesId = Quantiles.documentId(jobId);
        LOGGER.trace("ES API CALL: get ID {} type {} from index {}", quantilesId, Quantiles.TYPE.getPreferredName(), indexName);
        get(jobId, indexName, Quantiles.TYPE.getPreferredName(), quantilesId, handler, errorHandler, Quantiles.PARSER, () -> {
            LOGGER.info("There are currently no quantiles for job " + jobId);
            return null;
        });
    }

    /**
     * Get a job's model snapshot by its id
     */
    public void getModelSnapshot(String jobId, @Nullable String modelSnapshotId, Consumer<ModelSnapshot> handler,
                                 Consumer<Exception> errorHandler) {
        if (modelSnapshotId == null) {
            handler.accept(null);
            return;
        }
        get(jobId, AnomalyDetectorsIndex.jobResultsAliasedName(jobId), ModelSnapshot.TYPE.getPreferredName(),
                ModelSnapshot.documentId(jobId, modelSnapshotId), handler, errorHandler, ModelSnapshot.PARSER, () -> null);
    }

    /**
     * Get model snapshots for the job ordered by descending timestamp (newest first).
     *
     * @param jobId the job id
     * @param from  number of snapshots to from
     * @param size  number of snapshots to retrieve
     */
    public void modelSnapshots(String jobId, int from, int size, Consumer<QueryPage<ModelSnapshot>> handler,
                               Consumer<Exception> errorHandler) {
        modelSnapshots(jobId, from, size, null, true, QueryBuilders.matchAllQuery(), handler, errorHandler);
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
     */
    public void modelSnapshots(String jobId,
                               int from,
                               int size,
                               String startEpochMs,
                               String endEpochMs,
                               String sortField,
                               boolean sortDescending,
                               String snapshotId,
                               String description,
                               Consumer<QueryPage<ModelSnapshot>> handler,
                               Consumer<Exception> errorHandler) {
        boolean haveId = snapshotId != null && !snapshotId.isEmpty();
        boolean haveDescription = description != null && !description.isEmpty();
        ResultsFilterBuilder fb;
        if (haveId || haveDescription) {
            BoolQueryBuilder query = QueryBuilders.boolQuery();
            if (haveId) {
                query.filter(QueryBuilders.termQuery(ModelSnapshot.SNAPSHOT_ID.getPreferredName(), snapshotId));
            }
            if (haveDescription) {
                query.filter(QueryBuilders.termQuery(ModelSnapshot.DESCRIPTION.getPreferredName(), description));
            }

            fb = new ResultsFilterBuilder(query);
        } else {
            fb = new ResultsFilterBuilder();
        }

        QueryBuilder qb = fb.timeRange(Result.TIMESTAMP.getPreferredName(), startEpochMs, endEpochMs).build();
        modelSnapshots(jobId, from, size, sortField, sortDescending, qb, handler, errorHandler);
    }

    private void modelSnapshots(String jobId,
                                int from,
                                int size,
                                String sortField,
                                boolean sortDescending,
                                QueryBuilder qb,
                                Consumer<QueryPage<ModelSnapshot>> handler,
                                Consumer<Exception> errorHandler) {
        if (Strings.isEmpty(sortField)) {
            sortField = ModelSnapshot.TIMESTAMP.getPreferredName();
        }

        FieldSortBuilder sb = new FieldSortBuilder(sortField)
                .order(sortDescending ? SortOrder.DESC : SortOrder.ASC);

        String indexName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
        LOGGER.trace("ES API CALL: search all of type {} from index {} sort ascending {} with filter after sort from {} size {}",
                ModelSnapshot.TYPE, indexName, sortField, from, size);

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.indicesOptions(addIgnoreUnavailable(searchRequest.indicesOptions()));
        searchRequest.types(ModelSnapshot.TYPE.getPreferredName());
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.sort(sb);
        sourceBuilder.query(qb);
        sourceBuilder.from(from);
        sourceBuilder.size(size);
        searchRequest.source(sourceBuilder);
        client.search(searchRequest, ActionListener.wrap(searchResponse -> {
            List<ModelSnapshot> results = new ArrayList<>();
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                results.add(ModelSnapshot.fromJson(hit.getSourceRef()));
            }

            QueryPage<ModelSnapshot> result =
                    new QueryPage<>(results, searchResponse.getHits().getTotalHits(), ModelSnapshot.RESULTS_FIELD);
            handler.accept(result);
        }, errorHandler));
    }

    /**
     * Given a model snapshot, get the corresponding state and write it to the supplied
     * stream.  If there are multiple state documents they are separated using <code>'\0'</code>
     * when written to the stream.
     *
     * @param jobId         the job id
     * @param modelSnapshot the model snapshot to be restored
     * @param restoreStream the stream to write the state to
     */
    public void restoreStateToStream(String jobId, ModelSnapshot modelSnapshot, OutputStream restoreStream) throws IOException {
        String indexName = AnomalyDetectorsIndex.jobStateIndexName();

        // First try to restore categorizer state.  There are no snapshots for this, so the IDs simply
        // count up until a document is not found.  It's NOT an error to have no categorizer state.
        int docNum = 0;
        while (true) {
            String docId = CategorizerState.categorizerStateDocId(jobId, ++docNum);

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
            String docId = String.format(Locale.ROOT, "%s#%d", ModelSnapshot.documentId(modelSnapshot), docNum);

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

    public QueryPage<ModelPlot> modelPlot(String jobId, int from, int size) {
        SearchResponse searchResponse;
        String indexName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
        LOGGER.trace("ES API CALL: search result type {} from index {} from {}, size {}",
                ModelPlot.RESULT_TYPE_VALUE, indexName, from, size);

        searchResponse = client.prepareSearch(indexName)
                .setIndicesOptions(addIgnoreUnavailable(SearchRequest.DEFAULT_INDICES_OPTIONS))
                .setTypes(Result.TYPE.getPreferredName())
                .setQuery(new TermsQueryBuilder(Result.RESULT_TYPE.getPreferredName(), ModelPlot.RESULT_TYPE_VALUE))
                .setFrom(from).setSize(size)
                .get();

        List<ModelPlot> results = new ArrayList<>();

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            BytesReference source = hit.getSourceRef();
            try (XContentParser parser = XContentFactory.xContent(source).createParser(NamedXContentRegistry.EMPTY, source)) {
                ModelPlot modelPlot = ModelPlot.PARSER.apply(parser, null);
                results.add(modelPlot);
            } catch (IOException e) {
                throw new ElasticsearchParseException("failed to parse modelPlot", e);
            }
        }

        return new QueryPage<>(results, searchResponse.getHits().getTotalHits(), ModelPlot.RESULTS_FIELD);
    }

    /**
     * Get the job's model size stats.
     */
    public void modelSizeStats(String jobId, Consumer<ModelSizeStats> handler, Consumer<Exception> errorHandler) {
        LOGGER.trace("ES API CALL: get result type {} ID {} for job {}",
                ModelSizeStats.RESULT_TYPE_VALUE, ModelSizeStats.RESULT_TYPE_FIELD, jobId);

        String indexName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
        get(jobId, indexName, Result.TYPE.getPreferredName(), ModelSizeStats.documentId(jobId),
                handler, errorHandler, (parser, context) -> ModelSizeStats.PARSER.apply(parser, null).build(),
                () -> {
                    LOGGER.trace("No memory usage details for job with id {}", jobId);
                    return null;
                });
    }

    /**
     * Retrieves the filter with the given {@code filterId} from the datastore.
     *
     * @param ids the id of the requested filter
     */
    public void getFilters(Consumer<Set<MlFilter>> handler, Consumer<Exception> errorHandler, Set<String> ids) {
        mget(ML_META_INDEX, MlFilter.TYPE.getPreferredName(), ids, handler, errorHandler, MlFilter.PARSER);
    }

    /**
     * Maps authorization failures when querying ML indexes to job-specific authorization failures attributed to the ML actions.
     * Works by replacing the action name with another provided by the caller, and appending the job ID.
     * This is designed to improve understandability when an admin has applied index or document level security to the .ml-anomalies
     * indexes to allow some users to have access to certain job results but not others.
     * For example, if user ml_test is allowed to see some results, but not the ones for job "farequote" then:
     *
     * action [indices:data/read/search] is unauthorized for user [ml_test]
     *
     * gets mapped to:
     *
     * action [cluster:monitor/ml/anomaly_detectors/results/buckets/get] is unauthorized for user [ml_test] for job [farequote]
     *
     * Exceptions that are not related to authorization are returned unaltered.
     * @param e An exception that occurred while getting ML data
     * @param jobId The job ID
     * @param mappedActionName The outermost action name, that will make sense to the user who made the request
     */
    static Exception mapAuthFailure(Exception e, String jobId, String mappedActionName) {
        if (e instanceof ElasticsearchStatusException) {
            if (((ElasticsearchStatusException)e).status() == RestStatus.FORBIDDEN) {
                e = Exceptions.authorizationError(
                        e.getMessage().replaceFirst("action \\[.*?\\]", "action [" + mappedActionName + "]") + " for job [{}]", jobId);
            }
        }
        return e;
    }
}
