/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms.pivot;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerTransformStats;
import org.elasticsearch.xpack.core.dataframe.transforms.SourceConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.pivot.GroupConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.pivot.PivotConfig;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class Pivot {
    public static final int DEFAULT_INITIAL_PAGE_SIZE = 500;
    public static final int TEST_QUERY_PAGE_SIZE = 50;

    private static final String COMPOSITE_AGGREGATION_NAME = "_data_frame";

    private final PivotConfig config;

    // objects for re-using
    private final CompositeAggregationBuilder cachedCompositeAggregation;

    public Pivot(PivotConfig config) {
        this.config = config;
        this.cachedCompositeAggregation = createCompositeAggregation(config);
    }

    public void validate(Client client, SourceConfig sourceConfig, final ActionListener<Boolean> listener) {
        // step 1: check if used aggregations are supported
        for (AggregationBuilder agg : config.getAggregationConfig().getAggregatorFactories()) {
            if (Aggregations.isSupportedByDataframe(agg.getType()) == false) {
                listener.onFailure(new RuntimeException("Unsupported aggregation type [" + agg.getType() + "]"));
                return;
            }
        }

        // step 2: run a query to validate that config is valid
        runTestQuery(client, sourceConfig, listener);
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
     * Initially this returns a default, in future it might inspect the configuration and base the initial size
     * on the aggregations used.
     *
     * @return the page size
     */
    public int getInitialPageSize() {
        return DEFAULT_INITIAL_PAGE_SIZE;
    }

    public SearchRequest buildSearchRequest(SourceConfig sourceConfig, Map<String, Object> position, int pageSize) {
        QueryBuilder queryBuilder = sourceConfig.getQueryConfig().getQuery();

        SearchRequest searchRequest = new SearchRequest(sourceConfig.getIndex());
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(buildAggregation(position, pageSize));
        sourceBuilder.size(0);
        sourceBuilder.query(queryBuilder);
        searchRequest.source(sourceBuilder);
        return searchRequest;

    }

    public AggregationBuilder buildAggregation(Map<String, Object> position, int pageSize) {
        cachedCompositeAggregation.aggregateAfter(position);
        cachedCompositeAggregation.size(pageSize);

        return cachedCompositeAggregation;
    }

    public Stream<Map<String, Object>> extractResults(CompositeAggregation agg,
                                                      Map<String, String> fieldTypeMap,
                                                      DataFrameIndexerTransformStats dataFrameIndexerTransformStats) {

        GroupConfig groups = config.getGroupConfig();
        Collection<AggregationBuilder> aggregationBuilders = config.getAggregationConfig().getAggregatorFactories();
        Collection<PipelineAggregationBuilder> pipelineAggregationBuilders = config.getAggregationConfig().getPipelineAggregatorFactories();

        return AggregationResultUtils.extractCompositeAggregationResults(agg,
            groups,
            aggregationBuilders,
            pipelineAggregationBuilders,
            fieldTypeMap,
            dataFrameIndexerTransformStats);
    }

    private void runTestQuery(Client client, SourceConfig sourceConfig, final ActionListener<Boolean> listener) {
        SearchRequest searchRequest = buildSearchRequest(sourceConfig, null, TEST_QUERY_PAGE_SIZE);

        client.execute(SearchAction.INSTANCE, searchRequest, ActionListener.wrap(response -> {
            if (response == null) {
                listener.onFailure(new RuntimeException("Unexpected null response from test query"));
                return;
            }
            if (response.status() != RestStatus.OK) {
                listener.onFailure(new RuntimeException("Unexpected status from response of test query: " + response.status()));
                return;
            }
            listener.onResponse(true);
        }, e->{
            listener.onFailure(new RuntimeException("Failed to test query", e));
        }));
    }

    private static CompositeAggregationBuilder createCompositeAggregation(PivotConfig config) {
        CompositeAggregationBuilder compositeAggregation;

        try (XContentBuilder builder = jsonBuilder()) {
            // write configuration for composite aggs into builder
            config.toCompositeAggXContent(builder, ToXContentObject.EMPTY_PARAMS);
            XContentParser parser = builder.generator().contentType().xContent().createParser(NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(builder).streamInput());
            compositeAggregation = CompositeAggregationBuilder.parse(COMPOSITE_AGGREGATION_NAME, parser);
            config.getAggregationConfig().getAggregatorFactories().forEach(agg -> compositeAggregation.subAggregation(agg));
            config.getAggregationConfig().getPipelineAggregatorFactories().forEach(agg -> compositeAggregation.subAggregation(agg));
        } catch (IOException e) {
            throw new RuntimeException(DataFrameMessages.DATA_FRAME_TRANSFORM_PIVOT_FAILED_TO_CREATE_COMPOSITE_AGGREGATION, e);
        }
        return compositeAggregation;
    }
}
