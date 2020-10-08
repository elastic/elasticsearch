/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms.pivot;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GroupConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.SingleGroupSource;
import org.elasticsearch.xpack.transform.Transform;
import org.elasticsearch.xpack.transform.transforms.Function;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class Pivot implements Function {
    public static final int TEST_QUERY_PAGE_SIZE = 50;

    private static final String COMPOSITE_AGGREGATION_NAME = "_transform";
    private static final Logger logger = LogManager.getLogger(Pivot.class);

    private final PivotConfig config;
    private final String transformId;
    private final boolean supportsIncrementalBucketUpdate;

    // objects for re-using
    private final CompositeAggregationBuilder cachedCompositeAggregation;

    public Pivot(PivotConfig config, String transformId) {
        this.config = config;
        this.transformId = transformId;
        this.cachedCompositeAggregation = createCompositeAggregation(config);

        boolean supportsIncrementalBucketUpdate = false;
        for (Entry<String, SingleGroupSource> entry : config.getGroupConfig().getGroups().entrySet()) {
            supportsIncrementalBucketUpdate |= entry.getValue().supportsIncrementalBucketUpdate();
        }

        this.supportsIncrementalBucketUpdate = supportsIncrementalBucketUpdate;
    }

    @Override
    public void validateConfig(ActionListener<Boolean> listener) {
        for (AggregationBuilder agg : config.getAggregationConfig().getAggregatorFactories()) {
            if (TransformAggregations.isSupportedByTransform(agg.getType()) == false) {
                // todo: change to ValidationException
                listener.onFailure(
                    new ElasticsearchStatusException("Unsupported aggregation type [" + agg.getType() + "]", RestStatus.BAD_REQUEST)
                );
                return;
            }
        }
        listener.onResponse(true);
    }

    @Override
    public void validateQuery(Client client, SourceConfig sourceConfig, final ActionListener<Boolean> listener) {
        SearchRequest searchRequest = buildSearchRequest(sourceConfig, null, TEST_QUERY_PAGE_SIZE);

        client.execute(SearchAction.INSTANCE, searchRequest, ActionListener.wrap(response -> {
            if (response == null) {
                listener.onFailure(
                    new ElasticsearchStatusException("Unexpected null response from test query", RestStatus.SERVICE_UNAVAILABLE)
                );
                return;
            }
            if (response.status() != RestStatus.OK) {
                listener.onFailure(
                    new ElasticsearchStatusException(
                        "Unexpected status from response of test query: " + response.status(),
                        response.status()
                    )
                );
                return;
            }
            listener.onResponse(true);
        }, e -> {
            Throwable unwrapped = ExceptionsHelper.unwrapCause(e);
            RestStatus status = unwrapped instanceof ElasticsearchException
                ? ((ElasticsearchException) unwrapped).status()
                : RestStatus.SERVICE_UNAVAILABLE;
            listener.onFailure(new ElasticsearchStatusException("Failed to test query", status, unwrapped));
        }));
    }

    @Override
    public void deduceMappings(Client client, SourceConfig sourceConfig, final ActionListener<Map<String, String>> listener) {
        SchemaUtil.deduceMappings(client, config, sourceConfig.getIndex(), listener);
    }

    @Override
    public void preview(
        Client client,
        Map<String, String> headers,
        SourceConfig sourceConfig,
        Map<String, String> fieldTypeMap,
        int numberOfBuckets,
        ActionListener<List<Map<String, Object>>> listener
    ) {
        ClientHelper.executeWithHeadersAsync(
            headers,
            ClientHelper.TRANSFORM_ORIGIN,
            client,
            SearchAction.INSTANCE,
            buildSearchRequest(sourceConfig, null, numberOfBuckets),
            ActionListener.wrap(r -> {
                try {
                    final Aggregations aggregations = r.getAggregations();
                    if (aggregations == null) {
                        listener.onFailure(
                            new ElasticsearchStatusException("Source indices have been deleted or closed.", RestStatus.BAD_REQUEST)
                        );
                        return;
                    }
                    final CompositeAggregation agg = aggregations.get(COMPOSITE_AGGREGATION_NAME);
                    TransformIndexerStats stats = new TransformIndexerStats();
                    // remove all internal fields

                    List<Map<String, Object>> docs = extractResults(agg, fieldTypeMap, stats).peek(
                        doc -> doc.keySet().removeIf(k -> k.startsWith("_"))
                    ).collect(Collectors.toList());

                    listener.onResponse(docs);
                } catch (AggregationResultUtils.AggregationExtractionException extractionException) {
                    listener.onFailure(new ElasticsearchStatusException(extractionException.getMessage(), RestStatus.BAD_REQUEST));
                }
            }, listener::onFailure)
        );
    }

    /**
     * Get the initial page size for this pivot.
     *
     * The page size is the main parameter for adjusting memory consumption. Memory consumption mainly depends on
     * the page size, the type of aggregations and the data. As the page size is the number of buckets we return
     * per page the page size is a multiplier for the costs of aggregating bucket.
     *
     * The user may set a maximum in the {@link PivotConfig#getMaxPageSearchSize()}, but if that is not provided,
     *    the default {@link Transform#DEFAULT_INITIAL_MAX_PAGE_SEARCH_SIZE} is used.
     *
     * In future we might inspect the configuration and base the initial size on the aggregations used.
     *
     * @return the page size
     */
    @Override
    public int getInitialPageSize() {
        return config.getMaxPageSearchSize() == null ? Transform.DEFAULT_INITIAL_MAX_PAGE_SEARCH_SIZE : config.getMaxPageSearchSize();
    }

    public SearchRequest buildSearchRequest(SourceConfig sourceConfig, Map<String, Object> position, int pageSize) {
        QueryBuilder queryBuilder = sourceConfig.getQueryConfig().getQuery();

        SearchRequest searchRequest = new SearchRequest(sourceConfig.getIndex());
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        buildSearchQuery(sourceBuilder, null, pageSize);
        sourceBuilder.query(queryBuilder);
        searchRequest.source(sourceBuilder);
        searchRequest.indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);

        logger.trace("Search request: {}", searchRequest);
        return searchRequest;
    }

    @Override
    public SearchSourceBuilder buildSearchQuery(SearchSourceBuilder builder, Map<String, Object> position, int pageSize) {
        cachedCompositeAggregation.aggregateAfter(position);
        cachedCompositeAggregation.size(pageSize);

        return builder.size(0).aggregation(cachedCompositeAggregation);
    }

    @Override
    public ChangeCollector buildChangeCollector(String synchronizationField) {
        CompositeAggregationBuilder aggregationBuilder = null;

        // skip if none of the group_by's requires it
        if (supportsIncrementalBucketUpdate()) {
            aggregationBuilder = createCompositeAggregationSources(config, true);
        }

        return CompositeBucketsChangeCollector.buildChangeCollector(
            aggregationBuilder,
            config.getGroupConfig().getGroups(),
            synchronizationField
        );
    }

    @Override
    public boolean supportsIncrementalBucketUpdate() {
        return supportsIncrementalBucketUpdate;
    }

    public Stream<Map<String, Object>> extractResults(
        CompositeAggregation agg,
        Map<String, String> fieldTypeMap,
        TransformIndexerStats transformIndexerStats
    ) {
        GroupConfig groups = config.getGroupConfig();
        Collection<AggregationBuilder> aggregationBuilders = config.getAggregationConfig().getAggregatorFactories();
        Collection<PipelineAggregationBuilder> pipelineAggregationBuilders = config.getAggregationConfig().getPipelineAggregatorFactories();

        return AggregationResultUtils.extractCompositeAggregationResults(
            agg,
            groups,
            aggregationBuilders,
            pipelineAggregationBuilders,
            fieldTypeMap,
            transformIndexerStats
        );
    }

    @Override
    public Tuple<Stream<IndexRequest>, Map<String, Object>> processSearchResponse(
        final SearchResponse searchResponse,
        final String destinationIndex,
        final String destinationPipeline,
        final Map<String, String> fieldMappings,
        final TransformIndexerStats stats
    ) {
        final Aggregations aggregations = searchResponse.getAggregations();

        // Treat this as a "we reached the end".
        // This should only happen when all underlying indices have gone away. Consequently, there is no more data to read.
        if (aggregations == null) {
            return null;
        }

        final CompositeAggregation compositeAgg = aggregations.get(COMPOSITE_AGGREGATION_NAME);
        if (compositeAgg == null || compositeAgg.getBuckets().isEmpty()) {
            return null;
        }

        return new Tuple<>(
            processBucketsToIndexRequests(compositeAgg, destinationIndex, destinationPipeline, fieldMappings, stats),
            compositeAgg.afterKey()
        );
    }

    @Override
    public SearchSourceBuilder buildSearchQueryForInitialProgress(SearchSourceBuilder searchSourceBuilder) {
        BoolQueryBuilder existsClauses = QueryBuilders.boolQuery();

        config.getGroupConfig().getGroups().values().forEach(src -> {
            if (src.getMissingBucket() == false && src.getField() != null) {
                existsClauses.must(QueryBuilders.existsQuery(src.getField()));
            }
        });

        return searchSourceBuilder.query(existsClauses).size(0).trackTotalHits(true);
    }

    @Override
    public void getInitialProgressFromResponse(SearchResponse response, ActionListener<TransformProgress> progressListener) {
        progressListener.onResponse(new TransformProgress(response.getHits().getTotalHits().value, 0L, 0L));
    }

    /*
     * Parses the result and creates a stream of indexable documents
     *
     * Implementation decisions:
     *
     * Extraction uses generic maps as intermediate exchange format in order to hook in ingest pipelines/processors
     * in later versions, see {@link IngestDocument).
     */
    private Stream<IndexRequest> processBucketsToIndexRequests(
        CompositeAggregation agg,
        String destinationIndex,
        String destinationPipeline,
        Map<String, String> fieldMappings,
        TransformIndexerStats stats
    ) {
        return extractResults(agg, fieldMappings, stats).map(document -> {
            String id = (String) document.get(TransformField.DOCUMENT_ID_FIELD);

            if (id == null) {
                throw new RuntimeException("Expected a document id but got null.");
            }

            XContentBuilder builder;
            try {
                builder = jsonBuilder();
                builder.startObject();
                for (Map.Entry<String, ?> value : document.entrySet()) {
                    // skip all internal fields
                    if (value.getKey().startsWith("_") == false) {
                        builder.field(value.getKey(), value.getValue());
                    }
                }
                builder.endObject();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            IndexRequest request = new IndexRequest(destinationIndex).source(builder).id(id);
            if (destinationPipeline != null) {
                request.setPipeline(destinationPipeline);
            }
            return request;
        });
    }

    private static CompositeAggregationBuilder createCompositeAggregation(PivotConfig config) {
        final CompositeAggregationBuilder compositeAggregation = createCompositeAggregationSources(config, false);

        config.getAggregationConfig().getAggregatorFactories().forEach(agg -> compositeAggregation.subAggregation(agg));
        config.getAggregationConfig().getPipelineAggregatorFactories().forEach(agg -> compositeAggregation.subAggregation(agg));

        return compositeAggregation;
    }

    private static CompositeAggregationBuilder createCompositeAggregationSources(PivotConfig config, boolean forChangeDetection) {
        CompositeAggregationBuilder compositeAggregation;

        try (XContentBuilder builder = jsonBuilder()) {
            config.toCompositeAggXContent(builder, forChangeDetection);
            XContentParser parser = builder.generator()
                .contentType()
                .xContent()
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(builder).streamInput());
            compositeAggregation = CompositeAggregationBuilder.PARSER.parse(parser, COMPOSITE_AGGREGATION_NAME);
        } catch (IOException e) {
            throw new RuntimeException(TransformMessages.TRANSFORM_PIVOT_FAILED_TO_CREATE_COMPOSITE_AGGREGATION, e);
        }
        return compositeAggregation;
    }

}
