/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms.latest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesAction;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHits;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.core.transform.transforms.latest.LatestConfig;
import org.elasticsearch.xpack.transform.transforms.Function;
import org.elasticsearch.xpack.transform.transforms.IDGenerator;
import org.elasticsearch.xpack.transform.transforms.common.DocumentConversionUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class Latest implements Function {

    public static final int DEFAULT_INITIAL_MAX_PAGE_SEARCH_SIZE = 5000;
    public static final int TEST_QUERY_PAGE_SIZE = 50;

    private static final String COMPOSITE_AGGREGATION_NAME = "_transform";
    private static final String TOP_HITS_AGGREGATION_NAME = "_top_hits";
    private static final Logger logger = LogManager.getLogger(Latest.class);

    private final LatestConfig config;

    // objects for re-using
    private final CompositeAggregationBuilder cachedCompositeAggregation;

    public Latest(LatestConfig config) {
        this.config = config;
        this.cachedCompositeAggregation = createCompositeAggregation(config);
    }

    private static CompositeAggregationBuilder createCompositeAggregation(LatestConfig config) {
        List<CompositeValuesSourceBuilder<?>> sources =
            config.getUniqueKey().stream()
                .map(field -> new TermsValuesSourceBuilder(field).field(field).missingBucket(true))
                .collect(toList());
        TopHitsAggregationBuilder topHitsAgg =
            AggregationBuilders.topHits(TOP_HITS_AGGREGATION_NAME)
                .size(1)  // we are only interested in the top-1
                .sorts(config.getSorts());  // we copy the sort config directly from the function config
        return AggregationBuilders.composite(COMPOSITE_AGGREGATION_NAME, sources).subAggregation(topHitsAgg);
    }

    @Override
    public int getInitialPageSize() {
        return DEFAULT_INITIAL_MAX_PAGE_SEARCH_SIZE;
    }

    @Override
    public SearchSourceBuilder buildSearchQuery(SearchSourceBuilder builder, Map<String, Object> position, int pageSize) {
        cachedCompositeAggregation.aggregateAfter(position);
        cachedCompositeAggregation.size(pageSize);
        return builder.size(0).aggregation(cachedCompositeAggregation);
    }

    @Override
    public ChangeCollector buildChangeCollector(String synchronizationField) {
        return new LatestChangeCollector(synchronizationField);
    }

    private static Map<String, Object> convertBucketToDocument(CompositeAggregation.Bucket bucket,
                                                               LatestConfig config,
                                                               TransformIndexerStats transformIndexerStats) {
        transformIndexerStats.incrementNumDocuments(bucket.getDocCount());

        TopHits topHits = bucket.getAggregations().get(TOP_HITS_AGGREGATION_NAME);
        if (topHits.getHits().getHits().length != 1) {
            throw new ElasticsearchException(
                "Unexpected number of hits in the top_hits aggregation result. Wanted: 1, was: {}", topHits.getHits().getHits().length);
        }
        Map<String, Object> document = topHits.getHits().getHits()[0].getSourceAsMap();

        // generator to create unique but deterministic document ids, so we
        // - do not create duplicates if we re-run after failure
        // - update documents
        IDGenerator idGen = new IDGenerator();
        config.getUniqueKey().forEach(field -> idGen.add(field, bucket.getKey().get(field)));

        document.put(TransformField.DOCUMENT_ID_FIELD, idGen.getID());
        return document;
    }

    @Override
    public Tuple<Stream<IndexRequest>, Map<String, Object>> processSearchResponse(
        SearchResponse searchResponse,
        String destinationIndex,
        String destinationPipeline,
        Map<String, String> fieldTypeMap,
        TransformIndexerStats stats
    ) {
        Aggregations aggregations = searchResponse.getAggregations();

        // Treat this as a "we reached the end".
        // This should only happen when all underlying indices have gone away. Consequently, there is no more data to read.
        if (aggregations == null) {
            return null;
        }

        CompositeAggregation compositeAgg = aggregations.get(COMPOSITE_AGGREGATION_NAME);
        if (compositeAgg == null || compositeAgg.getBuckets().isEmpty()) {
            return null;
        }

        Stream<IndexRequest> indexRequestStream =
            compositeAgg.getBuckets().stream()
                .map(bucket -> convertBucketToDocument(bucket, config, stats))
                .map(document -> DocumentConversionUtils.convertDocumentToIndexRequest(document, destinationIndex, destinationPipeline));
        return Tuple.tuple(indexRequestStream, compositeAgg.afterKey());
    }

    @Override
    public void validateQuery(Client client, SourceConfig sourceConfig, ActionListener<Boolean> listener) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(sourceConfig.getQueryConfig().getQuery());
        buildSearchQuery(sourceBuilder, null, TEST_QUERY_PAGE_SIZE);
        SearchRequest searchRequest =
            new SearchRequest(sourceConfig.getIndex())
                .source(sourceBuilder)
                .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);

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
                        "Unexpected status from response of test query: {}", response.status(), response.status())
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
    public void validateConfig(ActionListener<Boolean> listener) {
        listener.onResponse(true);
    }

    @Override
    public void deduceMappings(Client client, SourceConfig sourceConfig, ActionListener<Map<String, String>> listener) {
        FieldCapabilitiesRequest fieldCapabilitiesRequest =
            new FieldCapabilitiesRequest().indices(sourceConfig.getIndex())
                .fields("*")
                .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        client.execute(
            FieldCapabilitiesAction.INSTANCE,
            fieldCapabilitiesRequest,
            ActionListener.wrap(
                response -> {
                    listener.onResponse(
                        DocumentConversionUtils.removeInternalFields(DocumentConversionUtils.extractFieldMappings(response)));
                },
                listener::onFailure)
        );
    }

    @Override
    public void preview(
        Client client,
        Map<String, String> headers,
        SourceConfig sourceConfig,
        Map<String, String> mappings,
        int numberOfBuckets,
        ActionListener<List<Map<String, Object>>> listener
    ) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(sourceConfig.getQueryConfig().getQuery());
        buildSearchQuery(sourceBuilder, null, numberOfBuckets);
        SearchRequest searchRequest =
            new SearchRequest(sourceConfig.getIndex())
                .source(sourceBuilder)
                .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);

        ClientHelper.assertNoAuthorizationHeader(headers);
        ClientHelper.executeWithHeadersAsync(
            headers,
            ClientHelper.TRANSFORM_ORIGIN,
            client,
            SearchAction.INSTANCE,
            searchRequest,
            ActionListener.wrap(r -> {
                Aggregations aggregations = r.getAggregations();
                if (aggregations == null) {
                    listener.onFailure(
                        new ElasticsearchStatusException("Source indices have been deleted or closed.", RestStatus.BAD_REQUEST));
                    return;
                }
                CompositeAggregation compositeAgg = aggregations.get(COMPOSITE_AGGREGATION_NAME);
                TransformIndexerStats stats = new TransformIndexerStats();

                List<Map<String, Object>> docs =
                    compositeAgg.getBuckets().stream()
                        .map(bucket -> convertBucketToDocument(bucket, config, stats))
                        .map(DocumentConversionUtils::removeInternalFields)
                        .collect(toList());

                listener.onResponse(docs);
            }, listener::onFailure)
        );
    }

    @Override
    public SearchSourceBuilder buildSearchQueryForInitialProgress(SearchSourceBuilder searchSourceBuilder) {
        BoolQueryBuilder existsClauses = QueryBuilders.boolQuery();
        config.getUniqueKey().forEach(field -> existsClauses.must(QueryBuilders.existsQuery(field)));
        return searchSourceBuilder.query(existsClauses).size(0).trackTotalHits(true);
    }

    @Override
    public void getInitialProgressFromResponse(SearchResponse response, ActionListener<TransformProgress> progressListener) {
        progressListener.onResponse(new TransformProgress(response.getHits().getTotalHits().value, 0L, 0L));
    }
}
