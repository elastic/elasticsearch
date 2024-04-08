/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.common;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.transform.transforms.Function;
import org.elasticsearch.xpack.transform.transforms.pivot.AggregationResultUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.core.Strings.format;

/**
 * Basic abstract class for implementing a transform function that utilizes composite aggregations
 */
public abstract class AbstractCompositeAggFunction implements Function {

    public static final int TEST_QUERY_PAGE_SIZE = 50;
    public static final String COMPOSITE_AGGREGATION_NAME = "_transform";

    private final CompositeAggregationBuilder cachedCompositeAggregation;

    public AbstractCompositeAggFunction(CompositeAggregationBuilder compositeAggregationBuilder) {
        cachedCompositeAggregation = compositeAggregationBuilder;
    }

    @Override
    public SearchSourceBuilder buildSearchQuery(SearchSourceBuilder builder, Map<String, Object> position, int pageSize) {
        cachedCompositeAggregation.aggregateAfter(position);
        cachedCompositeAggregation.size(pageSize);
        return builder.size(0).aggregation(cachedCompositeAggregation);
    }

    @Override
    public void preview(
        Client client,
        TimeValue timeout,
        Map<String, String> headers,
        SourceConfig sourceConfig,
        Map<String, String> fieldTypeMap,
        int numberOfBuckets,
        ActionListener<List<Map<String, Object>>> listener
    ) {
        ClientHelper.assertNoAuthorizationHeader(headers);
        ClientHelper.executeWithHeadersAsync(
            headers,
            ClientHelper.TRANSFORM_ORIGIN,
            client,
            TransportSearchAction.TYPE,
            buildSearchRequest(sourceConfig, timeout, numberOfBuckets),
            ActionListener.wrap(r -> {
                try {
                    final InternalAggregations aggregations = r.getAggregations();
                    if (aggregations == null) {
                        listener.onFailure(
                            new ElasticsearchStatusException("Source indices have been deleted or closed.", RestStatus.BAD_REQUEST)
                        );
                        return;
                    }
                    final CompositeAggregation agg = aggregations.get(COMPOSITE_AGGREGATION_NAME);
                    if (agg == null || agg.getBuckets().isEmpty()) {
                        listener.onResponse(Collections.emptyList());
                        return;
                    }

                    TransformIndexerStats stats = new TransformIndexerStats();
                    TransformProgress progress = new TransformProgress();
                    List<Map<String, Object>> docs = extractResults(agg, fieldTypeMap, stats, progress).map(
                        this::documentTransformationFunction
                    ).collect(Collectors.toList());

                    listener.onResponse(docs);
                } catch (AggregationResultUtils.AggregationExtractionException extractionException) {
                    listener.onFailure(new ElasticsearchStatusException(extractionException.getMessage(), RestStatus.BAD_REQUEST));
                }
            }, listener::onFailure)
        );
    }

    @Override
    public void validateQuery(
        Client client,
        Map<String, String> headers,
        SourceConfig sourceConfig,
        TimeValue timeout,
        ActionListener<Boolean> listener
    ) {
        SearchRequest searchRequest = buildSearchRequest(sourceConfig, timeout, TEST_QUERY_PAGE_SIZE);
        ClientHelper.executeWithHeadersAsync(
            headers,
            ClientHelper.TRANSFORM_ORIGIN,
            client,
            TransportSearchAction.TYPE,
            searchRequest,
            ActionListener.wrap(response -> {
                if (response == null) {
                    listener.onFailure(new ValidationException().addValidationError("Unexpected null response from test query"));
                    return;
                }
                if (response.status() != RestStatus.OK) {
                    listener.onFailure(
                        new ValidationException().addValidationError(
                            format("Unexpected status from response of test query: %s", response.status())
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
                listener.onFailure(
                    new ValidationException(unwrapped).addValidationError(format("Failed to test query, received status: %s", status))
                );
            })
        );
    }

    @Override
    public Tuple<Stream<IndexRequest>, Map<String, Object>> processSearchResponse(
        SearchResponse searchResponse,
        String destinationIndex,
        String destinationPipeline,
        Map<String, String> fieldTypeMap,
        TransformIndexerStats stats,
        TransformProgress progress
    ) {
        InternalAggregations aggregations = searchResponse.getAggregations();

        // Treat this as a "we reached the end".
        // This should only happen when all underlying indices have gone away. Consequently, there is no more data to read.
        if (aggregations == null) {
            return null;
        }

        CompositeAggregation compositeAgg = aggregations.get(COMPOSITE_AGGREGATION_NAME);
        if (compositeAgg == null || compositeAgg.afterKey() == null) {
            return null;
        }

        Stream<IndexRequest> indexRequestStream = extractResults(compositeAgg, fieldTypeMap, stats, progress).map(doc -> {
            String docId = (String) doc.remove(TransformField.DOCUMENT_ID_FIELD);
            return DocumentConversionUtils.convertDocumentToIndexRequest(
                docId,
                documentTransformationFunction(doc),
                destinationIndex,
                destinationPipeline
            );
        });

        return Tuple.tuple(indexRequestStream, compositeAgg.afterKey());
    }

    protected abstract Map<String, Object> documentTransformationFunction(Map<String, Object> document);

    protected abstract Stream<Map<String, Object>> extractResults(
        CompositeAggregation agg,
        Map<String, String> fieldTypeMap,
        TransformIndexerStats transformIndexerStats,
        TransformProgress progress
    );

    private SearchRequest buildSearchRequest(SourceConfig sourceConfig, TimeValue timeout, int pageSize) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(sourceConfig.getQueryConfig().getQuery())
            .runtimeMappings(sourceConfig.getRuntimeMappings())
            .timeout(timeout);
        buildSearchQuery(sourceBuilder, null, pageSize);
        return new SearchRequest(sourceConfig.getIndex()).source(sourceBuilder).indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
    }

    @Override
    public void getInitialProgressFromResponse(SearchResponse response, ActionListener<TransformProgress> progressListener) {
        progressListener.onResponse(new TransformProgress(response.getHits().getTotalHits().value, 0L, 0L));
    }

}
