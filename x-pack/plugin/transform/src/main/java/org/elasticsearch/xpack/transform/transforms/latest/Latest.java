/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.latest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHits;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.core.transform.transforms.latest.LatestConfig;
import org.elasticsearch.xpack.transform.transforms.IDGenerator;
import org.elasticsearch.xpack.transform.transforms.common.AbstractCompositeAggFunction;
import org.elasticsearch.xpack.transform.transforms.common.DocumentConversionUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;

/**
 * The latest transform function. This continually searches and processes results according to the passed {@link LatestConfig}
 */
public class Latest extends AbstractCompositeAggFunction {

    public static final int DEFAULT_INITIAL_MAX_PAGE_SEARCH_SIZE = 5000;

    private static final String TOP_HITS_AGGREGATION_NAME = "_top_hits";

    private final LatestConfig config;

    public Latest(LatestConfig config) {
        super(createCompositeAggregation(config));
        this.config = config;
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
    public ChangeCollector buildChangeCollector(String synchronizationField) {
        return new LatestChangeCollector(synchronizationField);
    }

    private static Map<String, Object> convertBucketToDocument(CompositeAggregation.Bucket bucket,
                                                               LatestConfig config,
                                                               TransformIndexerStats transformIndexerStats,
                                                               TransformProgress progress) {
        transformIndexerStats.incrementNumDocuments(bucket.getDocCount());
        if (progress != null) {
            progress.incrementDocsProcessed(bucket.getDocCount());
            progress.incrementDocsIndexed(1L);
        }

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
    public void validateConfig(ActionListener<Boolean> listener) {
        listener.onResponse(true);
    }

    @Override
    public List<String> getPerformanceCriticalFields() {
        return config.getUniqueKey();
    }

    @Override
    public void deduceMappings(Client client, SourceConfig sourceConfig, ActionListener<Map<String, String>> listener) {
        listener.onResponse(emptyMap());
    }

    @Override
    public SearchSourceBuilder buildSearchQueryForInitialProgress(SearchSourceBuilder searchSourceBuilder) {
        BoolQueryBuilder existsClauses = QueryBuilders.boolQuery();
        config.getUniqueKey().forEach(field -> existsClauses.must(QueryBuilders.existsQuery(field)));
        return searchSourceBuilder.query(existsClauses).size(0).trackTotalHits(true);
    }

    @Override
    protected Stream<Map<String, Object>> extractResults(
        CompositeAggregation agg,
        Map<String, String> fieldTypeMap,
        TransformIndexerStats transformIndexerStats,
        TransformProgress transformProgress
    ) {
        return agg.getBuckets().stream().map(bucket -> convertBucketToDocument(bucket, config, transformIndexerStats, transformProgress));
    }

    @Override
    protected Map<String, Object> documentTransformationFunction(Map<String, Object> document) {
        return DocumentConversionUtils.removeInternalFields(document);
    }
}
