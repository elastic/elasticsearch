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
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerTransformStats;
import org.elasticsearch.xpack.core.dataframe.transforms.pivot.GroupConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.pivot.PivotConfig;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.dataframe.transforms.DataFrameIndexer.COMPOSITE_AGGREGATION_NAME;

public class Pivot {
    private static final String COMPOSITE_AGGREGATION_NAME = "_data_frame";

    private final PivotConfig config;
    private final String source;

    // objects for re-using
    private final CompositeAggregationBuilder cachedCompositeAggregation;
    private final SearchRequest cachedSearchRequest;

    public Pivot(String source, QueryBuilder query, PivotConfig config) {
        this.source = source;
        this.config = config;
        this.cachedCompositeAggregation = createCompositeAggregation(config);
        this.cachedSearchRequest = createSearchRequest(source, query, cachedCompositeAggregation);
    }

    public void validate(Client client, final ActionListener<List<Map<String, Object>>> listener) {
        // step 1: check if used aggregations are supported
        for (AggregationBuilder agg : config.getAggregationConfig().getAggregatorFactories()) {
            if (Aggregations.isSupportedByDataframe(agg.getType()) == false) {
                listener.onFailure(new PivotException("Unsupported aggregation type [" + agg.getType() + "]"));
                return;
            }
        }

        // step 2: run a query to validate that config is valid
        runTestQuery(client, listener);
    }

    public void deduceMappings(Client client, final ActionListener<Map<String, String>> listener) {
        SchemaUtil.deduceMappings(client, config, source, listener);
    }

    public SearchRequest buildSearchRequest(Map<String, Object> position) {
        if (position != null) {
            cachedCompositeAggregation.aggregateAfter(position);
        }

        return cachedSearchRequest;
    }

    public Stream<Map<String, Object>> extractResults(CompositeAggregation agg,
            DataFrameIndexerTransformStats dataFrameIndexerTransformStats) {

        GroupConfig groups = config.getGroupConfig();
        Collection<AggregationBuilder> aggregationBuilders = config.getAggregationConfig().getAggregatorFactories();

        return AggregationResultUtils.extractCompositeAggregationResults(agg, groups, aggregationBuilders, dataFrameIndexerTransformStats);
    }

    private void runTestQuery(Client client, final ActionListener<List<Map<String, Object>>> listener) {
        // no after key
        cachedCompositeAggregation.aggregateAfter(null);
        client.execute(SearchAction.INSTANCE, cachedSearchRequest, ActionListener.wrap(
            response -> {
                if (response == null) {
                    listener.onFailure(new PivotException("Unexpected null response from test query"));
                    return;
                }
                if (response.status() != RestStatus.OK) {
                    listener.onFailure(new PivotException("Unexpected status from response of test query: " + response.status()));
                    return;
                }
                final CompositeAggregation agg = response.getAggregations().get(COMPOSITE_AGGREGATION_NAME);
                DataFrameIndexerTransformStats stats = new DataFrameIndexerTransformStats();
                listener.onResponse(extractResults(agg, stats).collect(Collectors.toList()));
            },
            e -> listener.onFailure(new PivotException("Failed to test query", e))
        ));
    }

    private static SearchRequest createSearchRequest(String index, QueryBuilder query, CompositeAggregationBuilder compositeAggregation) {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(compositeAggregation);
        sourceBuilder.size(0);
        sourceBuilder.query(query);
        searchRequest.source(sourceBuilder);
        return searchRequest;
    }

    private static CompositeAggregationBuilder createCompositeAggregation(PivotConfig config) {
        CompositeAggregationBuilder compositeAggregation;

        try (XContentBuilder builder = jsonBuilder()) {
            // write configuration for composite aggs into builder
            config.toCompositeAggXContent(builder, ToXContentObject.EMPTY_PARAMS);
            XContentParser parser = builder.generator().contentType().xContent().createParser(NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(builder).streamInput());
            compositeAggregation = CompositeAggregationBuilder.parse(COMPOSITE_AGGREGATION_NAME, parser);
            compositeAggregation.size(1000);
            config.getAggregationConfig().getAggregatorFactories().forEach(agg -> compositeAggregation.subAggregation(agg));
        } catch (IOException e) {
            throw new PivotException(DataFrameMessages.DATA_FRAME_TRANSFORM_PIVOT_FAILED_TO_CREATE_COMPOSITE_AGGREGATION, e);
        }
        return compositeAggregation;
    }
}
