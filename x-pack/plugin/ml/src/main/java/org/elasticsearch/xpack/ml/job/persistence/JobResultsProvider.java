/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.Stats;
import org.elasticsearch.search.aggregations.metrics.TopHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.MlMetaIndex;
import org.elasticsearch.xpack.core.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.core.ml.action.GetCategoriesAction;
import org.elasticsearch.xpack.core.ml.action.GetInfluencersAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetRecordsAction;
import org.elasticsearch.xpack.core.ml.calendars.Calendar;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStats;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.CategorizerState;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.CategorizerStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshotField;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.TimingStats;
import org.elasticsearch.xpack.core.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.job.results.CategoryDefinition;
import org.elasticsearch.xpack.core.ml.job.results.ForecastRequestStats;
import org.elasticsearch.xpack.core.ml.job.results.Influencer;
import org.elasticsearch.xpack.core.ml.job.results.ModelPlot;
import org.elasticsearch.xpack.core.ml.job.results.ReservedFieldNames;
import org.elasticsearch.xpack.core.ml.job.results.Result;
import org.elasticsearch.xpack.core.ml.stats.CountAccumulator;
import org.elasticsearch.xpack.core.ml.stats.ForecastStats;
import org.elasticsearch.xpack.core.ml.stats.StatsAccumulator;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.ml.job.categorization.GrokPatternCreator;
import org.elasticsearch.xpack.ml.job.persistence.InfluencersQueryBuilder.InfluencersQuery;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.AutodetectParams;
import org.elasticsearch.xpack.ml.utils.MlIndicesUtils;
import org.elasticsearch.xpack.ml.utils.persistence.MlParserUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.ml.job.messages.Messages.JOB_FORECAST_NATIVE_PROCESS_KILLED;

public class JobResultsProvider {
    private static final Logger LOGGER = LogManager.getLogger(JobResultsProvider.class);

    private static final int RECORDS_SIZE_PARAM = 10000;
    public static final int BUCKETS_FOR_ESTABLISHED_MEMORY_SIZE = 20;
    private static final double ESTABLISHED_MEMORY_CV_THRESHOLD = 0.1;

    // filter for quantiles in modelSnapshots to avoid memory overhead
    private static final FetchSourceContext REMOVE_QUANTILES_FROM_SOURCE = FetchSourceContext.of(
        true,
        null,
        new String[] { ModelSnapshot.QUANTILES.getPreferredName() }
    );

    private final Client client;
    private final Settings settings;
    private final IndexNameExpressionResolver resolver;

    public JobResultsProvider(Client client, Settings settings, IndexNameExpressionResolver resolver) {
        this.client = Objects.requireNonNull(client);
        this.settings = settings;
        this.resolver = resolver;
    }

    /**
     * Check that a previously deleted job with the same Id has not left any result
     * or categorizer state documents due to a failed delete. Any left over results would
     * appear to be part of the new job.
     *
     * We can't check for model state as the Id is based on the snapshot Id which is
     * a timestamp and so unpredictable however, it is unlikely a new job would have
     * the same snapshot Id as an old one.
     *
     * @param job  Job configuration
     * @param listener The ActionListener
     */
    public void checkForLeftOverDocuments(Job job, ActionListener<Boolean> listener) {

        SearchRequestBuilder stateDocSearch = client.prepareSearch(AnomalyDetectorsIndex.jobStateIndexPattern())
            .setQuery(
                QueryBuilders.idsQuery().addIds(CategorizerState.documentId(job.getId(), 1), CategorizerState.v54DocumentId(job.getId(), 1))
            )
            .setTrackTotalHits(false)
            .setIndicesOptions(IndicesOptions.strictExpand());

        SearchRequestBuilder quantilesDocSearch = client.prepareSearch(AnomalyDetectorsIndex.jobStateIndexPattern())
            .setQuery(QueryBuilders.idsQuery().addIds(Quantiles.documentId(job.getId()), Quantiles.v54DocumentId(job.getId())))
            .setTrackTotalHits(false)
            .setIndicesOptions(IndicesOptions.strictExpand());

        SearchRequestBuilder resultDocSearch = client.prepareSearch(AnomalyDetectorsIndex.jobResultsIndexPrefix() + "*")
            .setIndicesOptions(IndicesOptions.lenientExpandHidden())
            .setQuery(QueryBuilders.termQuery(Job.ID.getPreferredName(), job.getId()))
            .setTrackTotalHits(false)
            .setSize(1);

        MultiSearchRequestBuilder msearch = client.prepareMultiSearch().add(stateDocSearch).add(resultDocSearch).add(quantilesDocSearch);

        ActionListener<MultiSearchResponse> searchResponseActionListener = new DelegatingActionListener<>(listener) {
            @Override
            public void onResponse(MultiSearchResponse response) {
                List<SearchHit> searchHits = new ArrayList<>();
                // Consider the possibility that some of the responses are exceptions
                for (int i = 0; i < response.getResponses().length; i++) {
                    MultiSearchResponse.Item itemResponse = response.getResponses()[i];
                    if (itemResponse.isFailure()) {
                        Exception e = itemResponse.getFailure();
                        // There's a further complication, which is that msearch doesn't translate a
                        // closed index cluster block exception into a friendlier index closed exception
                        if (e instanceof ClusterBlockException cbe) {
                            for (ClusterBlock block : cbe.blocks()) {
                                if ("index closed".equals(block.description())) {
                                    SearchRequest searchRequest = msearch.request().requests().get(i);
                                    // Don't wrap the original exception, because then it would be the root cause
                                    // and Kibana would display it in preference to the friendlier exception
                                    e = ExceptionsHelper.badRequestException(
                                        "Cannot create job [{}] as it requires closed index {}",
                                        job.getId(),
                                        searchRequest.indices()
                                    );
                                }
                            }
                        }
                        delegate.onFailure(e);
                        return;
                    }
                    searchHits.addAll(Arrays.asList(itemResponse.getResponse().getHits().getHits()));
                }

                if (searchHits.isEmpty()) {
                    delegate.onResponse(true);
                } else {
                    int quantileDocCount = 0;
                    int categorizerStateDocCount = 0;
                    int resultDocCount = 0;
                    for (SearchHit hit : searchHits) {
                        if (hit.getId().equals(Quantiles.documentId(job.getId()))
                            || hit.getId().equals(Quantiles.v54DocumentId(job.getId()))) {
                            quantileDocCount++;
                        } else if (hit.getId().startsWith(CategorizerState.documentPrefix(job.getId()))
                            || hit.getId().startsWith(CategorizerState.v54DocumentPrefix(job.getId()))) {
                                categorizerStateDocCount++;
                            } else {
                                resultDocCount++;
                            }
                    }

                    LOGGER.warn(
                        "{} result, {} quantile state and {} categorizer state documents exist for a prior job with Id [{}]",
                        resultDocCount,
                        quantileDocCount,
                        categorizerStateDocCount,
                        job.getId()
                    );

                    delegate.onFailure(
                        ExceptionsHelper.conflictStatusException(
                            "["
                                + resultDocCount
                                + "] result and ["
                                + (quantileDocCount + categorizerStateDocCount)
                                + "] state documents exist for a prior job with Id ["
                                + job.getId()
                                + "]. "
                                + "Please create the job with a different Id"
                        )
                    );
                }
            }
        };

        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ML_ORIGIN,
            msearch.request(),
            searchResponseActionListener,
            client::multiSearch
        );
    }

    /**
     * Create the Elasticsearch index and the mappings
     */
    public void createJobResultIndex(Job job, ClusterState state, final ActionListener<Boolean> finalListener) {
        Collection<String> termFields = (job.getAnalysisConfig() != null) ? job.getAnalysisConfig().termFields() : Collections.emptyList();

        String readAliasName = AnomalyDetectorsIndex.jobResultsAliasedName(job.getId());
        String writeAliasName = AnomalyDetectorsIndex.resultsWriteAlias(job.getId());
        String tempIndexName = job.getInitialResultsIndexName();

        // Our read/write aliases should point to the concrete index
        // If the initial index is NOT an alias, either it is already a concrete index, or it does not exist yet
        if (state.getMetadata().hasAlias(tempIndexName)) {
            String[] concreteIndices = resolver.concreteIndexNames(state, IndicesOptions.lenientExpandOpen(), tempIndexName);

            // SHOULD NOT be closed as in typical call flow checkForLeftOverDocuments already verified this
            // if it is closed, we bailout and return an error
            if (concreteIndices.length == 0) {
                finalListener.onFailure(
                    ExceptionsHelper.badRequestException(
                        "Cannot create job [{}] as it requires closed index {}",
                        job.getId(),
                        tempIndexName
                    )
                );
                return;
            }
            tempIndexName = concreteIndices[0];
        }
        final String indexName = tempIndexName;

        ActionListener<Boolean> indexAndMappingsListener = ActionListener.wrap(success -> {
            final IndicesAliasesRequest request = client.admin()
                .indices()
                .prepareAliases()
                .addAliasAction(
                    IndicesAliasesRequest.AliasActions.add()
                        .index(indexName)
                        .alias(readAliasName)
                        .isHidden(true)
                        .filter(QueryBuilders.termQuery(Job.ID.getPreferredName(), job.getId()))
                )
                .addAliasAction(IndicesAliasesRequest.AliasActions.add().index(indexName).alias(writeAliasName).isHidden(true))
                .request();
            executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                ML_ORIGIN,
                request,
                ActionListener.<AcknowledgedResponse>wrap(r -> finalListener.onResponse(true), finalListener::onFailure),
                client.admin().indices()::aliases
            );
        }, finalListener::onFailure);

        // Indices can be shared, so only create if it doesn't exist already. Saves us a roundtrip if
        // already in the CS
        if (state.getMetadata().hasIndex(indexName) == false) {
            LOGGER.trace("ES API CALL: create index {}", indexName);
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
            executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                ML_ORIGIN,
                createIndexRequest,
                ActionListener.<CreateIndexResponse>wrap(
                    // Add the term field mappings and alias. The complication is that the state at the
                    // beginning of the operation doesn't have any knowledge of the index, as it's only
                    // just been created. So we need yet another operation to get the mappings for it.
                    r -> getLatestIndexMappingsAndAddTerms(indexName, termFields, indexAndMappingsListener),
                    e -> {
                        // Possible that the index was created while the request was executing,
                        // so we need to handle that possibility
                        if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                            LOGGER.info("Index [{}] already exists", indexName);
                            getLatestIndexMappingsAndAddTerms(indexName, termFields, indexAndMappingsListener);
                        } else {
                            finalListener.onFailure(e);
                        }
                    }
                ),
                client.admin().indices()::create
            );
        } else {
            MappingMetadata indexMappings = state.metadata().index(indexName).mapping();
            addTermsMapping(indexMappings, indexName, termFields, indexAndMappingsListener);
        }
    }

    private void getLatestIndexMappingsAndAddTerms(String indexName, Collection<String> termFields, ActionListener<Boolean> listener) {

        ActionListener<GetMappingsResponse> getMappingsListener = ActionListener.wrap(getMappingsResponse -> {
            // Expect one index. If this is not the case then it means the
            // index has been deleted almost immediately after being created, and this is
            // so unlikely that it's reasonable to fail the whole operation.
            MappingMetadata indexMappings = getMappingsResponse.getMappings().values().iterator().next();
            addTermsMapping(indexMappings, indexName, termFields, listener);
        }, listener::onFailure);

        GetMappingsRequest getMappingsRequest = client.admin().indices().prepareGetMappings(indexName).request();
        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ML_ORIGIN,
            getMappingsRequest,
            getMappingsListener,
            client.admin().indices()::getMappings
        );
    }

    private void addTermsMapping(
        MappingMetadata mapping,
        String indexName,
        Collection<String> termFields,
        ActionListener<Boolean> listener
    ) {
        long fieldCountLimit = MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.get(settings);

        if (violatedFieldCountLimit(termFields.size(), fieldCountLimit, mapping)) {
            String message = "Cannot create job in index '"
                + indexName
                + "' as the "
                + MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey()
                + " setting will be violated";
            listener.onFailure(new IllegalArgumentException(message));
        } else {
            updateIndexMappingWithTermFields(indexName, termFields, listener);
        }
    }

    public static boolean violatedFieldCountLimit(long additionalFieldCount, long fieldCountLimit, MappingMetadata mapping) {
        long numFields = countFields(mapping.sourceAsMap());
        return numFields + additionalFieldCount > fieldCountLimit;
    }

    @SuppressWarnings("unchecked")
    public static int countFields(Map<String, Object> mapping) {
        Object propertiesNode = mapping.get("properties");
        if (propertiesNode instanceof Map) {
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

        try (XContentBuilder termFieldsMapping = JsonXContent.contentBuilder()) {
            createTermFieldsMapping(termFieldsMapping, termFields);
            final PutMappingRequest request = client.admin().indices().preparePutMapping(indexName).setSource(termFieldsMapping).request();
            executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                ML_ORIGIN,
                request,
                listener.safeMap(AcknowledgedResponse::isAcknowledged),
                client.admin().indices()::putMapping
            );
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    // Visible for testing
    static void createTermFieldsMapping(XContentBuilder builder, Collection<String> termFields) throws IOException {
        builder.startObject();
        builder.startObject("properties");
        for (String fieldName : termFields) {
            if (ReservedFieldNames.isValidFieldName(fieldName)) {
                builder.startObject(fieldName).field(ElasticsearchMappings.TYPE, ElasticsearchMappings.KEYWORD).endObject();
            }
        }
        builder.endObject();
        builder.endObject();
    }

    /**
     * Get the job's data counts
     *
     * @param jobId The job id
     */
    public void dataCounts(String jobId, Consumer<DataCounts> handler, Consumer<Exception> errorHandler) {
        String indexName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
        searchSingleResult(
            jobId,
            DataCounts.TYPE.getPreferredName(),
            createLatestDataCountsSearch(indexName, jobId),
            DataCounts.PARSER,
            result -> handler.accept(result.result),
            errorHandler,
            () -> new DataCounts(jobId)
        );
    }

    public void getDataCountsModelSizeAndTimingStats(
        String jobId,
        @Nullable TaskId parentTaskId,
        TriConsumer<DataCounts, ModelSizeStats, TimingStats> handler,
        Consumer<Exception> errorHandler
    ) {
        final String results = "results";
        final String timingStats = "timing_stats";
        final String dataCounts = "data_counts";
        final String modelSizeStats = "model_size_stats";
        final String topHits = "hits";
        SearchRequest request = client.prepareSearch(AnomalyDetectorsIndex.jobResultsAliasedName(jobId))
            .setSize(0)
            .setTrackTotalHits(false)
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .addAggregation(
                AggregationBuilders.filters(
                    results,
                    new FiltersAggregator.KeyedFilter(
                        dataCounts,
                        QueryBuilders.idsQuery().addIds(DataCounts.documentId(jobId), DataCounts.v54DocumentId(jobId))
                    ),
                    new FiltersAggregator.KeyedFilter(timingStats, QueryBuilders.idsQuery().addIds(TimingStats.documentId(jobId))),
                    new FiltersAggregator.KeyedFilter(
                        modelSizeStats,
                        QueryBuilders.termQuery(Result.RESULT_TYPE.getPreferredName(), ModelSizeStats.RESULT_TYPE_VALUE)
                    )
                )
                    .subAggregation(
                        AggregationBuilders.topHits(topHits)
                            .size(1)
                            .sorts(
                                List.of(
                                    SortBuilders.fieldSort(DataCounts.LOG_TIME.getPreferredName())
                                        .order(SortOrder.DESC)
                                        .unmappedType(NumberFieldMapper.NumberType.LONG.typeName())
                                        .missing(0L),
                                    SortBuilders.fieldSort(TimingStats.BUCKET_COUNT.getPreferredName())
                                        .order(SortOrder.DESC)
                                        .unmappedType(NumberFieldMapper.NumberType.LONG.typeName())
                                        .missing(0L)
                                )
                            )
                    )
            )
            .request();
        if (parentTaskId != null) {
            request.setParentTask(parentTaskId);
        }
        executeAsyncWithOrigin(client.threadPool().getThreadContext(), ML_ORIGIN, request, ActionListener.<SearchResponse>wrap(response -> {
            Aggregations aggs = response.getAggregations();
            if (aggs == null) {
                handler.apply(new DataCounts(jobId), new ModelSizeStats.Builder(jobId).build(), new TimingStats(jobId));
                return;
            }
            Filters filters = aggs.get(results);
            TopHits dataCountHit = filters.getBucketByKey(dataCounts).getAggregations().get(topHits);
            DataCounts dataCountsResult = dataCountHit.getHits().getHits().length == 0
                ? new DataCounts(jobId)
                : MlParserUtils.parse(dataCountHit.getHits().getHits()[0], DataCounts.PARSER);

            TopHits timingStatsHits = filters.getBucketByKey(timingStats).getAggregations().get(topHits);
            TimingStats timingStatsResult = timingStatsHits.getHits().getHits().length == 0
                ? new TimingStats(jobId)
                : MlParserUtils.parse(timingStatsHits.getHits().getHits()[0], TimingStats.PARSER);

            TopHits modelSizeHits = filters.getBucketByKey(modelSizeStats).getAggregations().get(topHits);
            ModelSizeStats modelSizeStatsResult = modelSizeHits.getHits().getHits().length == 0
                ? new ModelSizeStats.Builder(jobId).build()
                : MlParserUtils.parse(modelSizeHits.getHits().getHits()[0], ModelSizeStats.LENIENT_PARSER).build();

            handler.apply(dataCountsResult, modelSizeStatsResult, timingStatsResult);
        }, errorHandler), client::search);
    }

    private SearchRequestBuilder createLatestDataCountsSearch(String indexName, String jobId) {
        return client.prepareSearch(indexName)
            .setSize(1)
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            // look for both old and new formats
            .setQuery(QueryBuilders.idsQuery().addIds(DataCounts.documentId(jobId), DataCounts.v54DocumentId(jobId)))
            // We want to sort on log_time. However, this was added a long time later and before that we used to
            // sort on latest_record_time. Thus we handle older data counts where no log_time exists and we fall back
            // to the prior behaviour.
            .addSort(
                SortBuilders.fieldSort(DataCounts.LOG_TIME.getPreferredName())
                    .order(SortOrder.DESC)
                    .unmappedType(NumberFieldMapper.NumberType.LONG.typeName())
                    .missing(0L)
            )
            .addSort(SortBuilders.fieldSort(DataCounts.LATEST_RECORD_TIME.getPreferredName()).order(SortOrder.DESC));
    }

    private SearchRequestBuilder createLatestTimingStatsSearch(String indexName, String jobId) {
        return client.prepareSearch(indexName)
            .setSize(1)
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setQuery(QueryBuilders.idsQuery().addIds(TimingStats.documentId(jobId)))
            .addSort(SortBuilders.fieldSort(TimingStats.BUCKET_COUNT.getPreferredName()).order(SortOrder.DESC));
    }

    public void datafeedTimingStats(
        List<String> jobIds,
        @Nullable TaskId parentTaskId,
        ActionListener<Map<String, DatafeedTimingStats>> listener
    ) {
        if (jobIds.isEmpty()) {
            listener.onResponse(Map.of());
            return;
        }
        MultiSearchRequestBuilder msearchRequestBuilder = client.prepareMultiSearch();
        for (String jobId : jobIds) {
            String indexName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
            msearchRequestBuilder.add(createLatestDatafeedTimingStatsSearch(indexName, jobId));
        }
        MultiSearchRequest msearchRequest = msearchRequestBuilder.request();
        if (parentTaskId != null) {
            msearchRequest.setParentTask(parentTaskId);
        }

        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ML_ORIGIN,
            msearchRequest,
            ActionListener.<MultiSearchResponse>wrap(msearchResponse -> {
                Map<String, DatafeedTimingStats> timingStatsByJobId = new HashMap<>();
                for (int i = 0; i < msearchResponse.getResponses().length; i++) {
                    String jobId = jobIds.get(i);
                    MultiSearchResponse.Item itemResponse = msearchResponse.getResponses()[i];
                    if (itemResponse.isFailure()) {
                        listener.onFailure(itemResponse.getFailure());
                        return;
                    }
                    SearchResponse searchResponse = itemResponse.getResponse();
                    ShardSearchFailure[] shardFailures = searchResponse.getShardFailures();
                    int unavailableShards = searchResponse.getTotalShards() - searchResponse.getSuccessfulShards();
                    if (CollectionUtils.isEmpty(shardFailures) == false) {
                        LOGGER.error("[{}] Search request returned shard failures: {}", jobId, Arrays.toString(shardFailures));
                        listener.onFailure(new ElasticsearchException(ExceptionsHelper.shardFailuresToErrorMsg(jobId, shardFailures)));
                        return;
                    }
                    if (unavailableShards > 0) {
                        listener.onFailure(
                            new ElasticsearchException(
                                "[" + jobId + "] Search request encountered [" + unavailableShards + "] unavailable shards"
                            )
                        );
                        return;
                    }
                    SearchHits hits = searchResponse.getHits();
                    long hitsCount = hits.getHits().length;
                    if (hitsCount == 0 || hitsCount > 1) {
                        SearchRequest searchRequest = msearchRequest.requests().get(i);
                        LOGGER.debug("Found {} hits for [{}]", hitsCount == 0 ? "0" : "multiple", new Object[] { searchRequest.indices() });
                        continue;
                    }
                    SearchHit hit = hits.getHits()[0];
                    try {
                        DatafeedTimingStats timingStats = MlParserUtils.parse(hit, DatafeedTimingStats.PARSER);
                        timingStatsByJobId.put(jobId, timingStats);
                    } catch (Exception e) {
                        listener.onFailure(e);
                        return;
                    }
                }
                listener.onResponse(timingStatsByJobId);
            }, listener::onFailure),
            client::multiSearch
        );
    }

    public void datafeedTimingStats(String jobId, Consumer<DatafeedTimingStats> handler, Consumer<Exception> errorHandler) {
        String indexName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
        searchSingleResult(
            jobId,
            DatafeedTimingStats.TYPE.getPreferredName(),
            createLatestDatafeedTimingStatsSearch(indexName, jobId),
            DatafeedTimingStats.PARSER,
            result -> handler.accept(result.result),
            errorHandler,
            () -> new DatafeedTimingStats(jobId)
        );
    }

    private SearchRequestBuilder createLatestDatafeedTimingStatsSearch(String indexName, String jobId) {
        return client.prepareSearch(indexName)
            .setSize(1)
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setQuery(QueryBuilders.idsQuery().addIds(DatafeedTimingStats.documentId(jobId)))
            .addSort(
                SortBuilders.fieldSort(DatafeedTimingStats.TOTAL_SEARCH_TIME_MS.getPreferredName())
                    .unmappedType("double")
                    .order(SortOrder.DESC)
            );
    }

    public void getAutodetectParams(Job job, String snapshotId, Consumer<AutodetectParams> consumer, Consumer<Exception> errorHandler) {
        String jobId = job.getId();

        ActionListener<AutodetectParams.Builder> getScheduledEventsListener = ActionListener.wrap(paramsBuilder -> {
            ScheduledEventsQueryBuilder scheduledEventsQueryBuilder = new ScheduledEventsQueryBuilder();
            scheduledEventsQueryBuilder.start(job.earliestValidTimestamp(paramsBuilder.getDataCounts()));
            scheduledEventsForJob(jobId, job.getGroups(), scheduledEventsQueryBuilder, ActionListener.wrap(events -> {
                paramsBuilder.setScheduledEvents(events.results());
                consumer.accept(paramsBuilder.build());
            }, errorHandler));
        }, errorHandler);

        AutodetectParams.Builder paramsBuilder = new AutodetectParams.Builder(job.getId());
        String resultsIndex = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
        String stateIndex = AnomalyDetectorsIndex.jobStateIndexPattern();

        MultiSearchRequestBuilder msearch = client.prepareMultiSearch()
            .add(createLatestDataCountsSearch(resultsIndex, jobId))
            .add(createLatestModelSizeStatsSearch(resultsIndex))
            .add(createLatestTimingStatsSearch(resultsIndex, jobId));

        if (snapshotId != null) {
            msearch.add(createDocIdSearch(resultsIndex, ModelSnapshot.documentId(jobId, snapshotId)));
            msearch.add(createDocIdSearch(stateIndex, Quantiles.documentId(jobId)));
        }

        for (String filterId : job.getAnalysisConfig().extractReferencedFilters()) {
            msearch.add(createDocIdSearch(MlMetaIndex.indexName(), MlFilter.documentId(filterId)));
        }

        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ML_ORIGIN,
            msearch.request(),
            ActionListener.<MultiSearchResponse>wrap(response -> {
                for (int i = 0; i < response.getResponses().length; i++) {
                    MultiSearchResponse.Item itemResponse = response.getResponses()[i];
                    if (itemResponse.isFailure()) {
                        errorHandler.accept(itemResponse.getFailure());
                        return;
                    }
                    SearchResponse searchResponse = itemResponse.getResponse();
                    ShardSearchFailure[] shardFailures = searchResponse.getShardFailures();
                    int unavailableShards = searchResponse.getTotalShards() - searchResponse.getSuccessfulShards();
                    if (CollectionUtils.isEmpty(shardFailures) == false) {
                        LOGGER.error("[{}] Search request returned shard failures: {}", jobId, Arrays.toString(shardFailures));
                        errorHandler.accept(new ElasticsearchException(ExceptionsHelper.shardFailuresToErrorMsg(jobId, shardFailures)));
                        return;
                    }
                    if (unavailableShards > 0) {
                        errorHandler.accept(
                            new ElasticsearchException(
                                "[" + jobId + "] Search request encountered [" + unavailableShards + "] unavailable shards"
                            )
                        );
                        return;
                    }
                    SearchHits hits = searchResponse.getHits();
                    long hitsCount = hits.getHits().length;
                    if (hitsCount == 0) {
                        SearchRequest searchRequest = msearch.request().requests().get(i);
                        LOGGER.debug("Found 0 hits for [{}]", new Object[] { searchRequest.indices() });
                    }
                    for (SearchHit hit : hits) {
                        try {
                            parseAutodetectParamSearchHit(jobId, paramsBuilder, hit);
                        } catch (Exception e) {
                            errorHandler.accept(e);
                            return;
                        }
                    }
                }
                getScheduledEventsListener.onResponse(paramsBuilder);
            }, errorHandler),
            client::multiSearch
        );
    }

    public void getAutodetectParams(Job job, Consumer<AutodetectParams> consumer, Consumer<Exception> errorHandler) {
        getAutodetectParams(job, job.getModelSnapshotId(), consumer, errorHandler);
    }

    private SearchRequestBuilder createDocIdSearch(String index, String id) {
        return client.prepareSearch(index)
            .setSize(1)
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setQuery(QueryBuilders.idsQuery().addIds(id))
            .setRouting(id);
    }

    /**
     * @throws ElasticsearchException when search hit cannot be parsed
     * @throws IllegalStateException when search hit has an unexpected ID
     */
    private static void parseAutodetectParamSearchHit(String jobId, AutodetectParams.Builder paramsBuilder, SearchHit hit) {
        String hitId = hit.getId();
        if (DataCounts.documentId(jobId).equals(hitId)) {
            paramsBuilder.setDataCounts(MlParserUtils.parse(hit, DataCounts.PARSER));
        } else if (TimingStats.documentId(jobId).equals(hitId)) {
            paramsBuilder.setTimingStats(MlParserUtils.parse(hit, TimingStats.PARSER));
        } else if (hitId.startsWith(ModelSizeStats.documentIdPrefix(jobId))) {
            ModelSizeStats.Builder modelSizeStats = MlParserUtils.parse(hit, ModelSizeStats.LENIENT_PARSER);
            paramsBuilder.setModelSizeStats(modelSizeStats == null ? null : modelSizeStats.build());
        } else if (hitId.startsWith(ModelSnapshot.documentIdPrefix(jobId))) {
            ModelSnapshot.Builder modelSnapshot = MlParserUtils.parse(hit, ModelSnapshot.LENIENT_PARSER);
            paramsBuilder.setModelSnapshot(modelSnapshot == null ? null : modelSnapshot.build());
        } else if (Quantiles.documentId(jobId).equals(hit.getId())) {
            paramsBuilder.setQuantiles(MlParserUtils.parse(hit, Quantiles.LENIENT_PARSER));
        } else if (hitId.startsWith(MlFilter.DOCUMENT_ID_PREFIX)) {
            paramsBuilder.addFilter(MlParserUtils.parse(hit, MlFilter.LENIENT_PARSER).build());
        } else {
            throw new IllegalStateException("Unexpected Id [" + hitId + "]");
        }
    }

    /**
     * Search for buckets with the parameters in the {@link BucketsQueryBuilder}
     * Uses the internal client, so runs as the _xpack user
     */
    public void bucketsViaInternalClient(
        String jobId,
        BucketsQueryBuilder query,
        Consumer<QueryPage<Bucket>> handler,
        Consumer<Exception> errorHandler
    ) {
        buckets(jobId, query, handler, errorHandler, client);
    }

    /**
     * Search for buckets with the parameters in the {@link BucketsQueryBuilder}
     * Uses a supplied client, so may run as the currently authenticated user
     */
    public void buckets(
        String jobId,
        BucketsQueryBuilder query,
        Consumer<QueryPage<Bucket>> handler,
        Consumer<Exception> errorHandler,
        @SuppressWarnings("HiddenField") Client client
    ) throws ResourceNotFoundException {

        String indexName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(query.build().trackTotalHits(true));
        searchRequest.indicesOptions(MlIndicesUtils.addIgnoreUnavailable(SearchRequest.DEFAULT_INDICES_OPTIONS));

        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ML_ORIGIN,
            searchRequest,
            ActionListener.<SearchResponse>wrap(searchResponse -> {
                SearchHits hits = searchResponse.getHits();
                List<Bucket> results = new ArrayList<>();
                for (SearchHit hit : hits.getHits()) {
                    BytesReference source = hit.getSourceRef();
                    try (
                        InputStream stream = source.streamInput();
                        XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                            .createParser(
                                XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE),
                                stream
                            )
                    ) {
                        Bucket bucket = Bucket.LENIENT_PARSER.apply(parser, null);
                        results.add(bucket);
                    } catch (IOException e) {
                        throw new ElasticsearchParseException("failed to parse bucket", e);
                    }
                }

                if (query.hasTimestamp() && results.isEmpty()) {
                    throw QueryPage.emptyQueryPage(Bucket.RESULTS_FIELD);
                }

                QueryPage<Bucket> buckets = new QueryPage<>(results, searchResponse.getHits().getTotalHits().value, Bucket.RESULTS_FIELD);

                if (query.isExpand()) {
                    Iterator<Bucket> bucketsToExpand = buckets.results()
                        .stream()
                        .filter(bucket -> bucket.getBucketInfluencers().size() > 0)
                        .iterator();
                    expandBuckets(jobId, query, buckets, bucketsToExpand, handler, errorHandler, client);
                } else {
                    handler.accept(buckets);
                }
            }, e -> errorHandler.accept(mapAuthFailure(e, jobId, GetBucketsAction.NAME))),
            client::search
        );
    }

    private void expandBuckets(
        String jobId,
        BucketsQueryBuilder query,
        QueryPage<Bucket> buckets,
        Iterator<Bucket> bucketsToExpand,
        Consumer<QueryPage<Bucket>> handler,
        Consumer<Exception> errorHandler,
        Client client
    ) {
        if (bucketsToExpand.hasNext()) {
            Consumer<Integer> c = i -> expandBuckets(jobId, query, buckets, bucketsToExpand, handler, errorHandler, client);
            expandBucket(jobId, query.isIncludeInterim(), bucketsToExpand.next(), c, errorHandler, client);
        } else {
            handler.accept(buckets);
        }
    }

    /**
     * Returns a {@link BatchedResultsIterator} that allows querying
     * and iterating over a large number of buckets of the given job.
     * The bucket and source indexes are returned by the iterator.
     *
     * @param jobId the id of the job for which buckets are requested
     * @return a bucket {@link BatchedResultsIterator}
     */
    public BatchedResultsIterator<Bucket> newBatchedBucketsIterator(String jobId) {
        return new BatchedBucketsIterator(new OriginSettingClient(client, ML_ORIGIN), jobId);
    }

    /**
     * Returns a {@link BatchedResultsIterator} that allows querying
     * and iterating over a large number of records in the given job
     * The records and source indexes are returned by the iterator.
     *
     * @param jobId the id of the job for which buckets are requested
     * @return a record {@link BatchedResultsIterator}
     */
    public BatchedResultsIterator<AnomalyRecord> newBatchedRecordsIterator(String jobId) {
        return new BatchedRecordsIterator(new OriginSettingClient(client, ML_ORIGIN), jobId);
    }

    /**
     * Expand a bucket with its records
     */
    // This now gets the first 10K records for a bucket. The rate of records per bucket
    // is controlled by parameter in the c++ process and its default value is 500. Users may
    // change that. Issue elastic/machine-learning-cpp#73 is open to prevent this.
    public void expandBucket(
        String jobId,
        boolean includeInterim,
        Bucket bucket,
        Consumer<Integer> consumer,
        Consumer<Exception> errorHandler,
        Client client
    ) {
        Consumer<QueryPage<AnomalyRecord>> h = page -> {
            bucket.getRecords().addAll(page.results());
            consumer.accept(bucket.getRecords().size());
        };
        bucketRecords(
            jobId,
            bucket,
            0,
            RECORDS_SIZE_PARAM,
            includeInterim,
            AnomalyRecord.PROBABILITY.getPreferredName(),
            false,
            h,
            errorHandler,
            client
        );
    }

    public void bucketRecords(
        String jobId,
        Bucket bucket,
        int from,
        int size,
        boolean includeInterim,
        String sortField,
        boolean descending,
        Consumer<QueryPage<AnomalyRecord>> handler,
        Consumer<Exception> errorHandler,
        Client client
    ) {
        // Find the records using the time stamp rather than a parent-child
        // relationship. The parent-child filter involves two queries behind
        // the scenes, and Elasticsearch documentation claims it's significantly
        // slower. Here we rely on the record timestamps being identical to the
        // bucket timestamp.
        RecordsQueryBuilder recordsQueryBuilder = new RecordsQueryBuilder().timestamp(bucket.getTimestamp())
            .from(from)
            .size(size)
            .includeInterim(includeInterim)
            .sortField(sortField)
            .sortDescending(descending);

        records(jobId, recordsQueryBuilder, handler, errorHandler, client);
    }

    /**
     * Get a page of {@linkplain CategoryDefinition}s for the given <code>jobId</code>.
     * Uses a supplied client, so may run as the currently authenticated user
     * @param jobId the job id
     * @param categoryId a specific category ID to retrieve, or <code>null</code> to retrieve as many as possible
     * @param partitionFieldValue the partition field value to filter on, or <code>null</code> for no filtering
     * @param augment Should the category definition be augmented with a Grok pattern?
     * @param from  Skip the first N categories. This parameter is for paging
     * @param size  Take only this number of categories
     * @param handler Consumer of the results
     * @param errorHandler Consumer of failures
     * @param parentTask Cancellable parent task if available
     * @param parentTaskId Parent task ID if available
     * @param client client with which to make search requests
     */
    public void categoryDefinitions(
        String jobId,
        Long categoryId,
        String partitionFieldValue,
        boolean augment,
        Integer from,
        Integer size,
        Consumer<QueryPage<CategoryDefinition>> handler,
        Consumer<Exception> errorHandler,
        @Nullable CancellableTask parentTask,
        @Nullable TaskId parentTaskId,
        Client client
    ) {
        if (categoryId != null && (from != null || size != null)) {
            throw new IllegalStateException("Both categoryId and pageParams are specified");
        }

        String indexName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
        LOGGER.trace(
            "ES API CALL: search all of category definitions from index {} sort ascending {} from {} size {}",
            indexName,
            CategoryDefinition.CATEGORY_ID.getPreferredName(),
            from,
            size
        );

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.indicesOptions(MlIndicesUtils.addIgnoreUnavailable(searchRequest.indicesOptions()));
        if (parentTaskId != null) {
            searchRequest.setParentTask(parentTaskId);
        }
        QueryBuilder categoryIdQuery;
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        if (categoryId != null) {
            categoryIdQuery = QueryBuilders.termQuery(CategoryDefinition.CATEGORY_ID.getPreferredName(), categoryId);
        } else if (from != null && size != null) {
            // Note: Even though category definitions currently have a result_type field, this was not the case for older versions
            // So, until at least 9.x, this existsQuery is still the preferred way to gather category definition objects
            categoryIdQuery = QueryBuilders.existsQuery(CategoryDefinition.CATEGORY_ID.getPreferredName());
            sourceBuilder.from(from)
                .size(size)
                .sort(new FieldSortBuilder(CategoryDefinition.CATEGORY_ID.getPreferredName()).order(SortOrder.ASC));
        } else {
            throw new IllegalStateException("Both categoryId and pageParams are not specified");
        }
        if (partitionFieldValue != null) {
            QueryBuilder partitionQuery = QueryBuilders.termQuery(
                CategoryDefinition.PARTITION_FIELD_VALUE.getPreferredName(),
                partitionFieldValue
            );
            QueryBuilder combinedQuery = QueryBuilders.boolQuery().must(categoryIdQuery).must(partitionQuery);
            sourceBuilder.query(combinedQuery);
        } else {
            sourceBuilder.query(categoryIdQuery);
        }
        sourceBuilder.trackTotalHits(true);
        searchRequest.source(sourceBuilder);
        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ML_ORIGIN,
            searchRequest,
            ActionListener.<SearchResponse>wrap(searchResponse -> {
                SearchHit[] hits = searchResponse.getHits().getHits();
                List<CategoryDefinition> results = new ArrayList<>(hits.length);
                for (SearchHit hit : hits) {
                    BytesReference source = hit.getSourceRef();
                    try (
                        InputStream stream = source.streamInput();
                        XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                            .createParser(
                                XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE),
                                stream
                            )
                    ) {
                        CategoryDefinition categoryDefinition = CategoryDefinition.LENIENT_PARSER.apply(parser, null);
                        // Check if parent task is cancelled as augmentation of many categories is a non-trivial task
                        if (parentTask != null && parentTask.isCancelled()) {
                            errorHandler.accept(
                                new TaskCancelledException(format("task cancelled with reason [%s]", parentTask.getReasonCancelled()))
                            );
                            return;
                        }
                        if (augment) {
                            augmentWithGrokPattern(categoryDefinition);
                        }
                        results.add(categoryDefinition);
                    } catch (IOException e) {
                        throw new ElasticsearchParseException("failed to parse category definition", e);
                    }
                }
                QueryPage<CategoryDefinition> result = new QueryPage<>(
                    results,
                    searchResponse.getHits().getTotalHits().value,
                    CategoryDefinition.RESULTS_FIELD
                );
                handler.accept(result);
            }, e -> errorHandler.accept(mapAuthFailure(e, jobId, GetCategoriesAction.NAME))),
            client::search
        );
    }

    void augmentWithGrokPattern(CategoryDefinition categoryDefinition) {
        List<String> examples = categoryDefinition.getExamples();
        String regex = categoryDefinition.getRegex();
        if (examples.isEmpty() || regex.isEmpty()) {
            categoryDefinition.setGrokPattern("");
        } else {
            categoryDefinition.setGrokPattern(
                GrokPatternCreator.findBestGrokMatchFromExamples(categoryDefinition.getJobId(), regex, examples)
            );
        }
    }

    /**
     * Search for anomaly records with the parameters in the
     * {@link RecordsQueryBuilder}
     * Uses a supplied client, so may run as the currently authenticated user
     */
    public void records(
        String jobId,
        RecordsQueryBuilder recordsQueryBuilder,
        Consumer<QueryPage<AnomalyRecord>> handler,
        Consumer<Exception> errorHandler,
        Client client
    ) {
        String indexName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);

        SearchSourceBuilder searchSourceBuilder = recordsQueryBuilder.build();
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.indicesOptions(MlIndicesUtils.addIgnoreUnavailable(searchRequest.indicesOptions()));
        searchRequest.source(recordsQueryBuilder.build().trackTotalHits(true));

        LOGGER.trace("ES API CALL: search all of records from index {} with query {}", indexName, searchSourceBuilder);
        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ML_ORIGIN,
            searchRequest,
            ActionListener.<SearchResponse>wrap(searchResponse -> {
                List<AnomalyRecord> results = new ArrayList<>();
                for (SearchHit hit : searchResponse.getHits().getHits()) {
                    BytesReference source = hit.getSourceRef();
                    try (
                        InputStream stream = source.streamInput();
                        XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                            .createParser(
                                XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE),
                                stream
                            )
                    ) {
                        results.add(AnomalyRecord.LENIENT_PARSER.apply(parser, null));
                    } catch (IOException e) {
                        throw new ElasticsearchParseException("failed to parse records", e);
                    }
                }
                QueryPage<AnomalyRecord> queryPage = new QueryPage<>(
                    results,
                    searchResponse.getHits().getTotalHits().value,
                    AnomalyRecord.RESULTS_FIELD
                );
                handler.accept(queryPage);
            }, e -> errorHandler.accept(mapAuthFailure(e, jobId, GetRecordsAction.NAME))),
            client::search
        );
    }

    /**
     * Return a page of influencers for the given job and within the given date range
     * Uses a supplied client, so may run as the currently authenticated user
     * @param jobId The job ID for which influencers are requested
     * @param query the query
     */
    public void influencers(
        String jobId,
        InfluencersQuery query,
        Consumer<QueryPage<Influencer>> handler,
        Consumer<Exception> errorHandler,
        Client client
    ) {
        QueryBuilder fb = new ResultsFilterBuilder().timeRange(Result.TIMESTAMP.getPreferredName(), query.getStart(), query.getEnd())
            .score(Influencer.INFLUENCER_SCORE.getPreferredName(), query.getInfluencerScoreFilter())
            .interim(query.isIncludeInterim())
            .build();

        String indexName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
        LOGGER.trace(
            "ES API CALL: search all of influencers from index {}{}  with filter from {} size {}",
            () -> indexName,
            () -> (query.getSortField() != null)
                ? " with sort " + (query.isSortDescending() ? "descending" : "ascending") + " on field " + query.getSortField()
                : "",
            query::getFrom,
            query::getSize
        );

        QueryBuilder qb = new BoolQueryBuilder().filter(fb)
            .filter(new TermsQueryBuilder(Result.RESULT_TYPE.getPreferredName(), Influencer.RESULT_TYPE_VALUE));

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.indicesOptions(MlIndicesUtils.addIgnoreUnavailable(searchRequest.indicesOptions()));
        FieldSortBuilder sb = query.getSortField() == null
            ? SortBuilders.fieldSort(ElasticsearchMappings.ES_DOC)
            : new FieldSortBuilder(query.getSortField()).order(query.isSortDescending() ? SortOrder.DESC : SortOrder.ASC);
        searchRequest.source(new SearchSourceBuilder().query(qb).from(query.getFrom()).size(query.getSize()).sort(sb).trackTotalHits(true));

        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ML_ORIGIN,
            searchRequest,
            ActionListener.<SearchResponse>wrap(response -> {
                List<Influencer> influencers = new ArrayList<>();
                for (SearchHit hit : response.getHits().getHits()) {
                    BytesReference source = hit.getSourceRef();
                    try (
                        InputStream stream = source.streamInput();
                        XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                            .createParser(
                                XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE),
                                stream
                            )
                    ) {
                        influencers.add(Influencer.LENIENT_PARSER.apply(parser, null));
                    } catch (IOException e) {
                        throw new ElasticsearchParseException("failed to parse influencer", e);
                    }
                }
                QueryPage<Influencer> result = new QueryPage<>(
                    influencers,
                    response.getHits().getTotalHits().value,
                    Influencer.RESULTS_FIELD
                );
                handler.accept(result);
            }, e -> errorHandler.accept(mapAuthFailure(e, jobId, GetInfluencersAction.NAME))),
            client::search
        );
    }

    /**
     * Returns a {@link BatchedResultsIterator} that allows querying
     * and iterating over a large number of influencers of the given job
     *
     * @param jobId the id of the job for which influencers are requested
     * @return an influencer {@link BatchedResultsIterator}
     */
    public BatchedResultsIterator<Influencer> newBatchedInfluencersIterator(String jobId) {
        return new BatchedInfluencersIterator(new OriginSettingClient(client, ML_ORIGIN), jobId);
    }

    /**
     * Get a job's model snapshot by its id
     */
    public void getModelSnapshot(
        String jobId,
        @Nullable String modelSnapshotId,
        Consumer<Result<ModelSnapshot>> handler,
        Consumer<Exception> errorHandler
    ) {
        if (modelSnapshotId == null) {
            handler.accept(null);
            return;
        }
        String resultsIndex = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
        SearchRequestBuilder search = createDocIdSearch(resultsIndex, ModelSnapshot.documentId(jobId, modelSnapshotId));
        searchSingleResult(
            jobId,
            ModelSnapshot.TYPE.getPreferredName(),
            search,
            ModelSnapshot.LENIENT_PARSER,
            result -> handler.accept(result.result == null ? null : new Result<>(result.index, result.result.build())),
            errorHandler,
            () -> null
        );
    }

    /**
     * Get model snapshots for the job ordered by descending timestamp (newest first).
     *
     * Note: quantiles are removed from the results.
     *
     * @param jobId the job id
     * @param from  number of snapshots to from
     * @param size  number of snapshots to retrieve
     */
    public void modelSnapshots(
        String jobId,
        int from,
        int size,
        Consumer<QueryPage<ModelSnapshot>> handler,
        Consumer<Exception> errorHandler
    ) {
        modelSnapshots(jobId, from, size, null, true, QueryBuilders.matchAllQuery(), null, handler, errorHandler);
    }

    /**
     * Get model snapshots for the job ordered by descending restore priority.
     *
     * Note: quantiles are removed from the results.
     *
     * @param jobId          the job id
     * @param from           number of snapshots to from
     * @param size           number of snapshots to retrieve
     * @param startEpochMs   earliest time to include (inclusive)
     * @param endEpochMs     latest time to include (exclusive)
     * @param sortField      optional sort field name (may be null)
     * @param sortDescending Sort in descending order
     * @param snapshotId     optional snapshot ID to match (null for all)
     * @param parentTaskId   the parent task ID if available
     * @param handler        consumer for the found model snapshot objects
     * @param errorHandler   consumer for any errors that occur
     */
    public void modelSnapshots(
        String jobId,
        int from,
        int size,
        String startEpochMs,
        String endEpochMs,
        String sortField,
        boolean sortDescending,
        String snapshotId,
        @Nullable TaskId parentTaskId,
        Consumer<QueryPage<ModelSnapshot>> handler,
        Consumer<Exception> errorHandler
    ) {
        String[] snapshotIds = Strings.splitStringByCommaToArray(snapshotId);
        QueryBuilder qb = new ResultsFilterBuilder().resourceTokenFilters(ModelSnapshotField.SNAPSHOT_ID.getPreferredName(), snapshotIds)
            .timeRange(Result.TIMESTAMP.getPreferredName(), startEpochMs, endEpochMs)
            .build();

        modelSnapshots(jobId, from, size, sortField, sortDescending, qb, parentTaskId, handler, errorHandler);
    }

    private void modelSnapshots(
        String jobId,
        int from,
        int size,
        String sortField,
        boolean sortDescending,
        QueryBuilder qb,
        @Nullable TaskId parentTaskId,
        Consumer<QueryPage<ModelSnapshot>> handler,
        Consumer<Exception> errorHandler
    ) {
        if (Strings.isEmpty(sortField)) {
            sortField = ModelSnapshot.TIMESTAMP.getPreferredName();
        }

        QueryBuilder finalQuery = QueryBuilders.boolQuery()
            .filter(QueryBuilders.existsQuery(ModelSnapshot.SNAPSHOT_DOC_COUNT.getPreferredName()))
            .must(qb);

        FieldSortBuilder sb = new FieldSortBuilder(sortField).order(sortDescending ? SortOrder.DESC : SortOrder.ASC);
        // `min_version` might not be present in very early snapshots.
        // Consequently, we should treat it as being at least from 6.3.0 or before
        // Also, if no jobs have been opened since the previous versions, the .ml-anomalies-* index may not have
        // the `min_version`.
        if (sortField.equals(ModelSnapshot.MIN_VERSION.getPreferredName())) {
            sb.missing("6.3.0").unmappedType("keyword");
        }

        String indexName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
        LOGGER.trace(
            "ES API CALL: search all model snapshots from index {} sort ascending {} with filter after sort from {} size {}",
            indexName,
            sortField,
            from,
            size
        );

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().sort(sb)
            .query(finalQuery)
            .from(from)
            .size(size)
            .trackTotalHits(true)
            .fetchSource(REMOVE_QUANTILES_FROM_SOURCE);
        SearchRequest searchRequest = new SearchRequest(indexName);
        if (parentTaskId != null) {
            searchRequest.setParentTask(parentTaskId);
        }
        searchRequest.indicesOptions(MlIndicesUtils.addIgnoreUnavailable(searchRequest.indicesOptions())).source(sourceBuilder);
        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ML_ORIGIN,
            searchRequest,
            ActionListener.<SearchResponse>wrap(searchResponse -> {
                List<ModelSnapshot> results = new ArrayList<>();
                for (SearchHit hit : searchResponse.getHits().getHits()) {
                    results.add(ModelSnapshot.fromJson(hit.getSourceRef()));
                }

                QueryPage<ModelSnapshot> result = new QueryPage<>(
                    results,
                    searchResponse.getHits().getTotalHits().value,
                    ModelSnapshot.RESULTS_FIELD
                );
                handler.accept(result);
            }, errorHandler),
            client::search
        );
    }

    public QueryPage<ModelPlot> modelPlot(String jobId, int from, int size) {
        SearchResponse searchResponse;
        String indexName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
        LOGGER.trace("ES API CALL: search model plots from index {} from {} size {}", indexName, from, size);

        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(ML_ORIGIN)) {
            searchResponse = client.prepareSearch(indexName)
                .setIndicesOptions(MlIndicesUtils.addIgnoreUnavailable(SearchRequest.DEFAULT_INDICES_OPTIONS))
                .setQuery(new TermsQueryBuilder(Result.RESULT_TYPE.getPreferredName(), ModelPlot.RESULT_TYPE_VALUE))
                .setFrom(from)
                .setSize(size)
                .setTrackTotalHits(true)
                .get();
        }

        List<ModelPlot> results = new ArrayList<>();

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            BytesReference source = hit.getSourceRef();
            try (
                InputStream stream = source.streamInput();
                XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                    .createParser(XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE), stream)
            ) {
                ModelPlot modelPlot = ModelPlot.LENIENT_PARSER.apply(parser, null);
                results.add(modelPlot);
            } catch (IOException e) {
                throw new ElasticsearchParseException("failed to parse modelPlot", e);
            }
        }

        return new QueryPage<>(results, searchResponse.getHits().getTotalHits().value, ModelPlot.RESULTS_FIELD);
    }

    public QueryPage<CategorizerStats> categorizerStats(String jobId, int from, int size) {
        SearchResponse searchResponse;
        String indexName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
        LOGGER.trace("ES API CALL: search categorizer stats from index {} from {} size {}", indexName, from, size);

        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(ML_ORIGIN)) {
            searchResponse = client.prepareSearch(indexName)
                .setIndicesOptions(MlIndicesUtils.addIgnoreUnavailable(SearchRequest.DEFAULT_INDICES_OPTIONS))
                .setQuery(new TermsQueryBuilder(Result.RESULT_TYPE.getPreferredName(), CategorizerStats.RESULT_TYPE_VALUE))
                .setFrom(from)
                .setSize(size)
                .setTrackTotalHits(true)
                .get();
        }

        List<CategorizerStats> results = new ArrayList<>();

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            BytesReference source = hit.getSourceRef();
            try (
                InputStream stream = source.streamInput();
                XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                    .createParser(XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE), stream)
            ) {
                CategorizerStats categorizerStats = CategorizerStats.LENIENT_PARSER.apply(parser, null).build();
                results.add(categorizerStats);
            } catch (IOException e) {
                throw new ElasticsearchParseException("failed to parse categorizerStats", e);
            }
        }

        return new QueryPage<>(results, searchResponse.getHits().getTotalHits().value, ModelPlot.RESULTS_FIELD);
    }

    /**
     * Get the job's model size stats.
     */
    public void modelSizeStats(String jobId, Consumer<ModelSizeStats> handler, Consumer<Exception> errorHandler) {
        LOGGER.trace("ES API CALL: search latest {} for job {}", ModelSizeStats.RESULT_TYPE_VALUE, jobId);

        String indexName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
        searchSingleResult(
            jobId,
            ModelSizeStats.RESULT_TYPE_VALUE,
            createLatestModelSizeStatsSearch(indexName),
            ModelSizeStats.LENIENT_PARSER,
            result -> handler.accept(result.result.build()),
            errorHandler,
            () -> new ModelSizeStats.Builder(jobId)
        );
    }

    private <U, T> void searchSingleResult(
        String jobId,
        String resultDescription,
        SearchRequestBuilder search,
        BiFunction<XContentParser, U, T> objectParser,
        Consumer<Result<T>> handler,
        Consumer<Exception> errorHandler,
        Supplier<T> notFoundSupplier
    ) {
        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ML_ORIGIN,
            search.request(),
            ActionListener.<SearchResponse>wrap(response -> {
                SearchHit[] hits = response.getHits().getHits();
                if (hits.length == 0) {
                    LOGGER.trace("No {} for job with id {}", resultDescription, jobId);
                    handler.accept(new Result<>(null, notFoundSupplier.get()));
                } else if (hits.length == 1) {
                    try {
                        T result = MlParserUtils.parse(hits[0], objectParser);
                        handler.accept(new Result<>(hits[0].getIndex(), result));
                    } catch (Exception e) {
                        errorHandler.accept(e);
                    }
                } else {
                    errorHandler.accept(
                        new IllegalStateException(
                            "Search for unique [" + resultDescription + "] returned [" + hits.length + "] hits even though size was 1"
                        )
                    );
                }
            }, errorHandler),
            client::search
        );
    }

    private SearchRequestBuilder createLatestModelSizeStatsSearch(String indexName) {
        return client.prepareSearch(indexName)
            .setSize(1)
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setQuery(QueryBuilders.termQuery(Result.RESULT_TYPE.getPreferredName(), ModelSizeStats.RESULT_TYPE_VALUE))
            .addSort(SortBuilders.fieldSort(ModelSizeStats.LOG_TIME_FIELD.getPreferredName()).order(SortOrder.DESC));
    }

    /**
     * Get the "established" memory usage of a job, if it has one.
     * In order for a job to be considered to have established memory usage it must:
     * - Have generated at least <code>BUCKETS_FOR_ESTABLISHED_MEMORY_SIZE</code> buckets of results
     * - Have generated at least one model size stats document
     * - Have low variability of model bytes in model size stats documents in the time period covered by the last
     *   <code>BUCKETS_FOR_ESTABLISHED_MEMORY_SIZE</code> buckets, which is defined as having a coefficient of variation
     *   of no more than <code>ESTABLISHED_MEMORY_CV_THRESHOLD</code>
     * If necessary this calculation will be done by performing searches against the results index.  However, the
     * calculation may have already been done in the C++ code, in which case the answer can just be read from the latest
     * model size stats.
     * @param jobId the id of the job for which established memory usage is required
     * @param latestBucketTimestamp the latest bucket timestamp to be used for the calculation, if known, otherwise
     *                              <code>null</code>, implying the latest bucket that exists in the results index
     * @param latestModelSizeStats the latest model size stats for the job, if known, otherwise <code>null</code> - supplying
     *                             these when available avoids one search
     * @param handler if the method succeeds, this will be passed the established memory usage (in bytes) of the
     *                specified job, or 0 if memory usage is not yet established
     * @param errorHandler if a problem occurs, the exception will be passed to this handler
     */
    public void getEstablishedMemoryUsage(
        String jobId,
        Date latestBucketTimestamp,
        ModelSizeStats latestModelSizeStats,
        Consumer<Long> handler,
        Consumer<Exception> errorHandler
    ) {

        if (latestModelSizeStats != null) {
            calculateEstablishedMemoryUsage(jobId, latestBucketTimestamp, latestModelSizeStats, handler, errorHandler);
        } else {
            modelSizeStats(
                jobId,
                modelSizeStats -> calculateEstablishedMemoryUsage(jobId, latestBucketTimestamp, modelSizeStats, handler, errorHandler),
                errorHandler
            );
        }
    }

    void calculateEstablishedMemoryUsage(
        String jobId,
        Date latestBucketTimestamp,
        ModelSizeStats latestModelSizeStats,
        Consumer<Long> handler,
        Consumer<Exception> errorHandler
    ) {

        assert latestModelSizeStats != null;

        // There might be an easy short-circuit if the latest model size stats say which number to use
        if (latestModelSizeStats.getAssignmentMemoryBasis() != null) {
            switch (latestModelSizeStats.getAssignmentMemoryBasis()) {
                case MODEL_MEMORY_LIMIT -> {
                    handler.accept(0L);
                    return;
                }
                case CURRENT_MODEL_BYTES -> {
                    handler.accept(latestModelSizeStats.getModelBytes());
                    return;
                }
                case PEAK_MODEL_BYTES -> {
                    Long storedPeak = latestModelSizeStats.getPeakModelBytes();
                    handler.accept((storedPeak != null) ? storedPeak : latestModelSizeStats.getModelBytes());
                    return;
                }
            }
        }

        String indexName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);

        // Step 2. Find the count, mean and standard deviation of memory usage over the time span of the last N bucket results,
        // where N is the number of buckets required to consider memory usage "established"
        Consumer<QueryPage<Bucket>> bucketHandler = buckets -> {
            if (buckets.results().size() == 1) {
                String searchFromTimeMs = Long.toString(buckets.results().get(0).getTimestamp().getTime());
                SearchRequestBuilder search = client.prepareSearch(indexName)
                    .setSize(0)
                    .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                    .setQuery(
                        new BoolQueryBuilder().filter(QueryBuilders.rangeQuery(Result.TIMESTAMP.getPreferredName()).gte(searchFromTimeMs))
                            .filter(QueryBuilders.termQuery(Result.RESULT_TYPE.getPreferredName(), ModelSizeStats.RESULT_TYPE_VALUE))
                    )
                    .addAggregation(AggregationBuilders.extendedStats("es").field(ModelSizeStats.MODEL_BYTES_FIELD.getPreferredName()));

                executeAsyncWithOrigin(
                    client.threadPool().getThreadContext(),
                    ML_ORIGIN,
                    search.request(),
                    ActionListener.<SearchResponse>wrap(response -> {
                        List<Aggregation> aggregations = response.getAggregations().asList();
                        if (aggregations.size() == 1) {
                            ExtendedStats extendedStats = (ExtendedStats) aggregations.get(0);
                            long count = extendedStats.getCount();
                            if (count <= 1) {
                                // model size stats either haven't changed in the last N buckets,
                                // so the latest (older) ones are established, or have only changed
                                // once, so again there's no recent variation
                                handler.accept(latestModelSizeStats.getModelBytes());
                            } else {
                                double coefficientOfVaration = extendedStats.getStdDeviation() / extendedStats.getAvg();
                                LOGGER.trace(
                                    "[{}] Coefficient of variation [{}] when calculating established memory use",
                                    jobId,
                                    coefficientOfVaration
                                );
                                // is there sufficient stability in the latest model size stats readings?
                                if (coefficientOfVaration <= ESTABLISHED_MEMORY_CV_THRESHOLD) {
                                    // yes, so return the latest model size as established
                                    handler.accept(latestModelSizeStats.getModelBytes());
                                } else {
                                    // no - we don't have an established model size
                                    handler.accept(0L);
                                }
                            }
                        } else {
                            handler.accept(0L);
                        }
                    }, errorHandler),
                    client::search
                );
            } else {
                LOGGER.trace("[{}] Insufficient history to calculate established memory use", jobId);
                handler.accept(0L);
            }
        };

        // Step 1. Find the time span of the most recent N bucket results, where N is the number of buckets
        // required to consider memory usage "established"
        BucketsQueryBuilder bucketQuery = new BucketsQueryBuilder().end(
            latestBucketTimestamp != null ? Long.toString(latestBucketTimestamp.getTime() + 1) : null
        )
            .sortField(Result.TIMESTAMP.getPreferredName())
            .sortDescending(true)
            .from(BUCKETS_FOR_ESTABLISHED_MEMORY_SIZE - 1)
            .size(1)
            .includeInterim(false);
        bucketsViaInternalClient(jobId, bucketQuery, bucketHandler, e -> {
            if (ExceptionsHelper.unwrapCause(e) instanceof ResourceNotFoundException) {
                handler.accept(0L);
            } else {
                errorHandler.accept(e);
            }
        });
    }

    public void scheduledEventsForJob(
        String jobId,
        List<String> jobGroups,
        ScheduledEventsQueryBuilder queryBuilder,
        ActionListener<QueryPage<ScheduledEvent>> handler
    ) {
        // Find all the calendars used by the job then the events for those calendars

        ActionListener<QueryPage<Calendar>> calendarsListener = ActionListener.wrap(calendars -> {
            if (calendars.results().isEmpty()) {
                handler.onResponse(new QueryPage<>(Collections.emptyList(), 0, ScheduledEvent.RESULTS_FIELD));
                return;
            }
            String[] calendarIds = calendars.results().stream().map(Calendar::getId).toArray(String[]::new);
            queryBuilder.calendarIds(calendarIds);
            scheduledEvents(queryBuilder, handler);
        }, handler::onFailure);

        CalendarQueryBuilder query = new CalendarQueryBuilder().jobId(jobId).jobGroups(jobGroups);
        calendars(query, calendarsListener);
    }

    public void scheduledEvents(ScheduledEventsQueryBuilder query, ActionListener<QueryPage<ScheduledEvent>> handler) {
        SearchRequestBuilder request = client.prepareSearch(MlMetaIndex.indexName())
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setSource(query.build())
            .setTrackTotalHits(true);

        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ML_ORIGIN,
            request.request(),
            ActionListener.<SearchResponse>wrap(response -> {
                List<ScheduledEvent> events = new ArrayList<>();
                SearchHit[] hits = response.getHits().getHits();
                try {
                    for (SearchHit hit : hits) {
                        ScheduledEvent.Builder event = MlParserUtils.parse(hit, ScheduledEvent.LENIENT_PARSER);

                        event.eventId(hit.getId());
                        events.add(event.build());
                    }
                    handler.onResponse(new QueryPage<>(events, response.getHits().getTotalHits().value, ScheduledEvent.RESULTS_FIELD));
                } catch (Exception e) {
                    handler.onFailure(e);
                }
            }, handler::onFailure),
            client::search
        );
    }

    public void setRunningForecastsToFailed(String jobId, ActionListener<Boolean> listener) {
        QueryBuilder forecastQuery = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery(Result.RESULT_TYPE.getPreferredName(), ForecastRequestStats.RESULT_TYPE_VALUE))
            .filter(QueryBuilders.termQuery(Job.ID.getPreferredName(), jobId))
            .filter(
                QueryBuilders.termsQuery(
                    ForecastRequestStats.STATUS.getPreferredName(),
                    ForecastRequestStats.ForecastRequestStatus.SCHEDULED.toString(),
                    ForecastRequestStats.ForecastRequestStatus.STARTED.toString()
                )
            );

        UpdateByQueryRequest request = new UpdateByQueryRequest(AnomalyDetectorsIndex.resultsWriteAlias(jobId)).setQuery(forecastQuery)
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setAbortOnVersionConflict(false)
            .setMaxRetries(3)
            .setRefresh(true)
            .setScript(
                new Script(
                    "ctx._source.forecast_status='failed';" + "ctx._source.forecast_messages=['" + JOB_FORECAST_NATIVE_PROCESS_KILLED + "']"
                )
            );

        executeAsyncWithOrigin(client, ML_ORIGIN, UpdateByQueryAction.INSTANCE, request, ActionListener.wrap(response -> {
            if (response.getUpdated() > 0) {
                LOGGER.warn("[{}] set [{}] forecasts to failed", jobId, response.getUpdated());
            }
            if (response.getBulkFailures().size() > 0) {
                LOGGER.warn(
                    "[{}] failed to set [{}] forecasts to failed. Bulk failures experienced {}",
                    jobId,
                    response.getTotal() - response.getUpdated(),
                    response.getBulkFailures().stream().map(BulkItemResponse.Failure::getMessage).collect(Collectors.toList())
                );
            }
            listener.onResponse(true);
        }, listener::onFailure));
    }

    public void getForecastRequestStats(
        String jobId,
        String forecastId,
        Consumer<ForecastRequestStats> handler,
        Consumer<Exception> errorHandler
    ) {
        String indexName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
        SearchRequestBuilder forecastSearch = client.prepareSearch(indexName)
            .setQuery(QueryBuilders.idsQuery().addIds(ForecastRequestStats.documentId(jobId, forecastId)));

        searchSingleResult(
            jobId,
            ForecastRequestStats.RESULTS_FIELD.getPreferredName(),
            forecastSearch,
            ForecastRequestStats.LENIENT_PARSER,
            result -> handler.accept(result.result),
            errorHandler,
            () -> null
        );
    }

    public void getForecastStats(
        String jobId,
        @Nullable TaskId parentTaskId,
        Consumer<ForecastStats> handler,
        Consumer<Exception> errorHandler
    ) {
        String indexName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);

        QueryBuilder termQuery = new TermsQueryBuilder(Result.RESULT_TYPE.getPreferredName(), ForecastRequestStats.RESULT_TYPE_VALUE);
        QueryBuilder jobQuery = new TermsQueryBuilder(Job.ID.getPreferredName(), jobId);
        QueryBuilder finalQuery = new BoolQueryBuilder().filter(termQuery).filter(jobQuery);

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.indicesOptions(MlIndicesUtils.addIgnoreUnavailable(searchRequest.indicesOptions()));
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(finalQuery);
        sourceBuilder.aggregation(
            AggregationBuilders.stats(ForecastStats.Fields.MEMORY).field(ForecastRequestStats.MEMORY_USAGE.getPreferredName())
        );
        sourceBuilder.aggregation(
            AggregationBuilders.stats(ForecastStats.Fields.RECORDS).field(ForecastRequestStats.PROCESSED_RECORD_COUNT.getPreferredName())
        );
        sourceBuilder.aggregation(
            AggregationBuilders.stats(ForecastStats.Fields.RUNTIME).field(ForecastRequestStats.PROCESSING_TIME_MS.getPreferredName())
        );
        sourceBuilder.aggregation(
            AggregationBuilders.terms(ForecastStats.Fields.STATUSES).field(ForecastRequestStats.STATUS.getPreferredName())
        );
        sourceBuilder.size(0);
        sourceBuilder.trackTotalHits(false);

        searchRequest.source(sourceBuilder);
        if (parentTaskId != null) {
            searchRequest.setParentTask(parentTaskId);
        }

        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ML_ORIGIN,
            searchRequest,
            ActionListener.<SearchResponse>wrap(searchResponse -> {
                Aggregations aggregations = searchResponse.getAggregations();
                if (aggregations == null) {
                    handler.accept(new ForecastStats());
                    return;
                }
                Map<String, Aggregation> aggregationsAsMap = aggregations.asMap();
                StatsAccumulator memoryStats = StatsAccumulator.fromStatsAggregation(
                    (Stats) aggregationsAsMap.get(ForecastStats.Fields.MEMORY)
                );
                Stats aggRecordsStats = (Stats) aggregationsAsMap.get(ForecastStats.Fields.RECORDS);
                // Stats already gives us all the counts and every doc as a "records" field.
                long totalHits = aggRecordsStats.getCount();
                StatsAccumulator recordStats = StatsAccumulator.fromStatsAggregation(aggRecordsStats);
                StatsAccumulator runtimeStats = StatsAccumulator.fromStatsAggregation(
                    (Stats) aggregationsAsMap.get(ForecastStats.Fields.RUNTIME)
                );
                CountAccumulator statusCount = CountAccumulator.fromTermsAggregation(
                    (StringTerms) aggregationsAsMap.get(ForecastStats.Fields.STATUSES)
                );

                ForecastStats forecastStats = new ForecastStats(totalHits, memoryStats, recordStats, runtimeStats, statusCount);
                handler.accept(forecastStats);
            }, errorHandler),
            client::search
        );

    }

    public void updateCalendar(
        String calendarId,
        Set<String> jobIdsToAdd,
        Set<String> jobIdsToRemove,
        Consumer<Calendar> handler,
        Consumer<Exception> errorHandler
    ) {

        ActionListener<Calendar> getCalendarListener = ActionListener.wrap(calendar -> {
            Set<String> currentJobs = new HashSet<>(calendar.getJobIds());

            for (String jobToRemove : jobIdsToRemove) {
                if (currentJobs.contains(jobToRemove) == false) {
                    errorHandler.accept(
                        ExceptionsHelper.badRequestException(
                            "Cannot remove [" + jobToRemove + "] as it is not present in calendar [" + calendarId + "]"
                        )
                    );
                    return;
                }
            }

            currentJobs.addAll(jobIdsToAdd);
            currentJobs.removeAll(jobIdsToRemove);
            Calendar updatedCalendar = new Calendar(calendar.getId(), new ArrayList<>(currentJobs), calendar.getDescription());

            UpdateRequest updateRequest = new UpdateRequest(MlMetaIndex.indexName(), updatedCalendar.documentId());
            updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                updateRequest.doc(updatedCalendar.toXContent(builder, ToXContent.EMPTY_PARAMS));
            } catch (IOException e) {
                throw new IllegalStateException("Failed to serialise calendar with id [" + updatedCalendar.getId() + "]", e);
            }

            executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                ML_ORIGIN,
                updateRequest,
                ActionListener.<UpdateResponse>wrap(response -> handler.accept(updatedCalendar), errorHandler),
                client::update
            );

        }, errorHandler);

        calendar(calendarId, getCalendarListener);
    }

    public void calendars(CalendarQueryBuilder queryBuilder, ActionListener<QueryPage<Calendar>> listener) {
        SearchRequest searchRequest = client.prepareSearch(MlMetaIndex.indexName())
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setTrackTotalHits(true)
            .setSource(queryBuilder.build())
            .request();

        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ML_ORIGIN,
            searchRequest,
            ActionListener.<SearchResponse>wrap(response -> {
                List<Calendar> calendars = new ArrayList<>();
                SearchHit[] hits = response.getHits().getHits();
                try {
                    if (queryBuilder.isForAllCalendars() == false && hits.length == 0) {
                        listener.onFailure(queryBuilder.buildNotFoundException());
                        return;
                    }
                    for (SearchHit hit : hits) {
                        calendars.add(MlParserUtils.parse(hit, Calendar.LENIENT_PARSER).build());
                    }
                    listener.onResponse(new QueryPage<>(calendars, response.getHits().getTotalHits().value, Calendar.RESULTS_FIELD));
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }, listener::onFailure),
            client::search
        );
    }

    public void removeJobFromCalendars(String jobId, ActionListener<Boolean> listener) {

        ActionListener<BulkResponse> updateCalendarsListener = ActionListener.wrap(
            r -> listener.onResponse(r.hasFailures() == false),
            listener::onFailure
        );

        ActionListener<QueryPage<Calendar>> getCalendarsListener = ActionListener.wrap(r -> {
            BulkRequestBuilder bulkUpdate = client.prepareBulk();
            bulkUpdate.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (Calendar calendar : r.results()) {
                List<String> ids = calendar.getJobIds().stream().filter(jId -> jobId.equals(jId) == false).collect(Collectors.toList());
                Calendar newCalendar = new Calendar(calendar.getId(), ids, calendar.getDescription());
                UpdateRequest updateRequest = new UpdateRequest(MlMetaIndex.indexName(), newCalendar.documentId());
                try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                    updateRequest.doc(newCalendar.toXContent(builder, ToXContent.EMPTY_PARAMS));
                } catch (IOException e) {
                    listener.onFailure(new IllegalStateException("Failed to serialise calendar with id [" + newCalendar.getId() + "]", e));
                    return;
                }
                bulkUpdate.add(updateRequest);
            }
            if (bulkUpdate.numberOfActions() > 0) {
                executeAsyncWithOrigin(client, ML_ORIGIN, BulkAction.INSTANCE, bulkUpdate.request(), updateCalendarsListener);
            } else {
                listener.onResponse(true);
            }
        }, listener::onFailure);

        CalendarQueryBuilder query = new CalendarQueryBuilder().jobId(jobId);
        calendars(query, getCalendarsListener);
    }

    public void calendar(String calendarId, ActionListener<Calendar> listener) {
        GetRequest getRequest = new GetRequest(MlMetaIndex.indexName(), Calendar.documentId(calendarId));
        executeAsyncWithOrigin(client.threadPool().getThreadContext(), ML_ORIGIN, getRequest, new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getDocResponse) {
                try {
                    if (getDocResponse.isExists()) {
                        BytesReference docSource = getDocResponse.getSourceAsBytesRef();
                        try (
                            InputStream stream = docSource.streamInput();
                            XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                                .createParser(
                                    XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE),
                                    stream
                                )
                        ) {
                            Calendar calendar = Calendar.LENIENT_PARSER.apply(parser, null).build();
                            listener.onResponse(calendar);
                        }
                    } else {
                        this.onFailure(new ResourceNotFoundException("No calendar with id [" + calendarId + "]"));
                    }
                } catch (Exception e) {
                    this.onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (ExceptionsHelper.unwrapCause(e) instanceof IndexNotFoundException) {
                    listener.onFailure(new ResourceNotFoundException("No calendar with id [" + calendarId + "]"));
                } else {
                    listener.onFailure(e);
                }
            }
        }, client::get);
    }

    /**
     * Returns information needed to decide how to restart a job from a datafeed
     * @param jobId the job id
     * @param listener the listener
     */
    public void getRestartTimeInfo(String jobId, ActionListener<RestartTimeInfo> listener) {
        AtomicReference<Bucket> latestFinalBucketHolder = new AtomicReference<>();

        ActionListener<GetJobsStatsAction.Response> jobStatsHandler = ActionListener.wrap(r -> {
            QueryPage<GetJobsStatsAction.Response.JobStats> page = r.getResponse();
            if (page.count() == 1) {
                DataCounts dataCounts = page.results().get(0).getDataCounts();
                listener.onResponse(
                    new RestartTimeInfo(
                        latestFinalBucketHolder.get() == null ? null : latestFinalBucketHolder.get().getTimestamp().getTime(),
                        dataCounts.getLatestRecordTimeStamp() == null ? null : dataCounts.getLatestRecordTimeStamp().getTime(),
                        dataCounts.getInputRecordCount() > 0
                    )
                );
            } else {
                listener.onFailure(
                    new IllegalStateException(
                        "[" + jobId + "] Incorrect response for job stats for single job: got [" + page.count() + "] stats."
                    )
                );
            }
        }, listener::onFailure);

        ActionListener<Bucket> latestFinalBucketListener = ActionListener.wrap(latestFinalBucket -> {
            latestFinalBucketHolder.set(latestFinalBucket);
            // If the job is open we must get the data counts from the running job instead of from the index,
            // as the data counts in the running job might be more recent than those that have been indexed. If
            // we go to the index it would be more efficient to just get the data counts rather than all stats.
            // However, we _shouldn't_ ever do this when starting a datafeed, because a precondition for starting
            // the datafeed is that its job is open. Therefore, it's reasonable to always go via the get job stats
            // endpoint, on the assumption it will almost always get the stats from memory. (The only edge case
            // where this won't happen is when the job fails during datafeed startup, and that should be very rare.)
            client.execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(jobId), jobStatsHandler);
        }, listener::onFailure);

        getLatestFinalBucket(jobId, latestFinalBucketListener);
    }

    private void getLatestFinalBucket(String jobId, ActionListener<Bucket> listener) {
        BucketsQueryBuilder latestBucketQuery = new BucketsQueryBuilder().sortField(Result.TIMESTAMP.getPreferredName())
            .sortDescending(true)
            .size(1)
            .includeInterim(false);
        bucketsViaInternalClient(jobId, latestBucketQuery, queryPage -> {
            if (queryPage.results().isEmpty()) {
                listener.onResponse(null);
            } else {
                listener.onResponse(queryPage.results().get(0));
            }
        }, listener::onFailure);
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
     * action [cluster:monitor/xpack/ml/anomaly_detectors/results/buckets/get] is unauthorized for user [ml_test] for job [farequote]
     *
     * Exceptions that are not related to authorization are returned unaltered.
     * @param e An exception that occurred while getting ML data
     * @param jobId The job ID
     * @param mappedActionName The outermost action name, that will make sense to the user who made the request
     */
    static Exception mapAuthFailure(Exception e, String jobId, String mappedActionName) {
        if (e instanceof ElasticsearchStatusException) {
            if (((ElasticsearchStatusException) e).status() == RestStatus.FORBIDDEN) {
                e = Exceptions.authorizationError(
                    e.getMessage().replaceFirst("action \\[.*?\\]", "action [" + mappedActionName + "]") + " for job [{}]",
                    jobId
                );
            }
        }
        return e;
    }
}
