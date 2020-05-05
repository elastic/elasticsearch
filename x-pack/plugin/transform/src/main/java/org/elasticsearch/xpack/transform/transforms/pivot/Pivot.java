/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms.pivot;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GroupConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.SingleGroupSource;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class Pivot {
    public static final int DEFAULT_INITIAL_PAGE_SIZE = 500;
    public static final int TEST_QUERY_PAGE_SIZE = 50;

    private static final String COMPOSITE_AGGREGATION_NAME = "_transform";
    private static final Logger logger = LogManager.getLogger(Pivot.class);

    private final PivotConfig config;
    private final boolean supportsIncrementalBucketUpdate;

    // objects for re-using
    private final CompositeAggregationBuilder cachedCompositeAggregation;

    public Pivot(PivotConfig config) {
        this.config = config;
        this.cachedCompositeAggregation = createCompositeAggregation(config);

        boolean supportsIncrementalBucketUpdate = false;
        for (Entry<String, SingleGroupSource> entry : config.getGroupConfig().getGroups().entrySet()) {
            supportsIncrementalBucketUpdate |= entry.getValue().supportsIncrementalBucketUpdate();
        }

        this.supportsIncrementalBucketUpdate = supportsIncrementalBucketUpdate;
    }

    public void validateConfig() {
        for (AggregationBuilder agg : config.getAggregationConfig().getAggregatorFactories()) {
            if (Aggregations.isSupportedByTransform(agg.getType()) == false) {
                throw new ElasticsearchStatusException("Unsupported aggregation type [" + agg.getType() + "]", RestStatus.BAD_REQUEST);
            }
        }
    }

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
            RestStatus status = unwrapped instanceof ElasticsearchException ?
                ((ElasticsearchException)unwrapped).status() :
                RestStatus.SERVICE_UNAVAILABLE;
            listener.onFailure(new ElasticsearchStatusException("Failed to test query",
                status,
                unwrapped));
        }));
    }

    public void deduceMappings(Client client, SourceConfig sourceConfig, final ActionListener<Map<String, String>> listener) {
        SchemaUtil.deduceMappings(client, config, sourceConfig.getIndex(), listener);
    }

    /**
     * Get the initial page size for this pivot.
     *
     * The page size is the main parameter for adjusting memory consumption. Memory consumption mainly depends on
     * the page size, the type of aggregations and the data. As the page size is the number of buckets we return
     * per page the page size is a multiplier for the costs of aggregating bucket.
     *
     * The user may set a maximum in the {@link PivotConfig#getMaxPageSearchSize()}, but if that is not provided,
     *    the default {@link Pivot#DEFAULT_INITIAL_PAGE_SIZE} is used.
     *
     * In future we might inspect the configuration and base the initial size on the aggregations used.
     *
     * @return the page size
     */
    public int getInitialPageSize() {
        return config.getMaxPageSearchSize() == null ? DEFAULT_INITIAL_PAGE_SIZE : config.getMaxPageSearchSize();
    }

    public SearchRequest buildSearchRequest(SourceConfig sourceConfig, Map<String, Object> position, int pageSize) {
        QueryBuilder queryBuilder = sourceConfig.getQueryConfig().getQuery();

        SearchRequest searchRequest = new SearchRequest(sourceConfig.getIndex());
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(buildAggregation(position, pageSize));
        sourceBuilder.size(0);
        sourceBuilder.query(queryBuilder);
        searchRequest.source(sourceBuilder);
        searchRequest.indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);

        logger.trace("Search request: {}", searchRequest);
        return searchRequest;
    }

    public AggregationBuilder buildAggregation(Map<String, Object> position, int pageSize) {
        cachedCompositeAggregation.aggregateAfter(position);
        cachedCompositeAggregation.size(pageSize);

        return cachedCompositeAggregation;
    }

    public CompositeAggregationBuilder buildIncrementalBucketUpdateAggregation(int pageSize) {

        CompositeAggregationBuilder compositeAgg = createCompositeAggregationSources(config, true);
        compositeAgg.size(pageSize);

        return compositeAgg;
    }

    public Map<String, Set<String>> initialIncrementalBucketUpdateMap() {

        Map<String, Set<String>> changedBuckets = new HashMap<>();
        for (Entry<String, SingleGroupSource> entry : config.getGroupConfig().getGroups().entrySet()) {
            if (entry.getValue().supportsIncrementalBucketUpdate()) {
                changedBuckets.put(entry.getKey(), new HashSet<>());
            }
        }

        return changedBuckets;
    }

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

    public QueryBuilder filterBuckets(
        Map<String, Set<String>> changedBuckets,
        String synchronizationField,
        long lastSynchronizationCheckpoint
    ) {
        assert changedBuckets != null;

        if (config.getGroupConfig().getGroups().size() == 1) {
            Entry<String, SingleGroupSource> entry = config.getGroupConfig().getGroups().entrySet().iterator().next();
            logger.trace(() -> new ParameterizedMessage("filter by bucket: {}/{}", entry.getKey(), entry.getValue().getField()));
            Set<String> changedBucketsByGroup = changedBuckets.get(entry.getKey());
            return entry.getValue()
                .getIncrementalBucketUpdateFilterQuery(changedBucketsByGroup, synchronizationField, lastSynchronizationCheckpoint);
        }

        // else: more than 1 group by, need to nest it
        BoolQueryBuilder filteredQuery = new BoolQueryBuilder();
        for (Entry<String, SingleGroupSource> entry : config.getGroupConfig().getGroups().entrySet()) {
            Set<String> changedBucketsByGroup = changedBuckets.get(entry.getKey());
            QueryBuilder sourceQueryFilter = entry.getValue()
                .getIncrementalBucketUpdateFilterQuery(changedBucketsByGroup, synchronizationField, lastSynchronizationCheckpoint);
            // the source might not define a filter optimization
            if (sourceQueryFilter != null) {
                filteredQuery.filter(sourceQueryFilter);
            }
        }

        return filteredQuery;
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
