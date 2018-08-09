/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.job;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.indexing.Iteration;
import org.elasticsearch.xpack.core.indexing.IterativeIndexer;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.job.DateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.GroupConfig;
import org.elasticsearch.xpack.core.rollup.job.RollupJob;
import org.elasticsearch.xpack.core.rollup.job.RollupJobConfig;
import org.elasticsearch.xpack.core.rollup.job.RollupJobStats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An abstract implementation of {@link IterativeIndexer} that builds a rollup index incrementally.
 */
public abstract class RollupIndexer extends IterativeIndexer<Map<String, Object> > {
    static final String AGGREGATION_NAME = RollupField.NAME;

    private final RollupJob job;
    protected final AtomicBoolean upgradedDocumentID;
    private final CompositeAggregationBuilder compositeBuilder;
    private long maxBoundary;

    /**
     * Ctr
     * @param executor Executor to use to fire the first request of a background job.
     * @param job The rollup job
     * @param initialState Initial state for the indexer
     * @param initialPosition The last indexed bucket of the task
     * @param upgradedDocumentID whether job has updated IDs (for BWC)
     */
    RollupIndexer(Executor executor, RollupJob job, AtomicReference<IndexerState> initialState, Map<String, Object> initialPosition,
            AtomicBoolean upgradedDocumentID) {
        super(executor, initialState, initialPosition, new RollupJobStats());
        this.job = job;
        this.compositeBuilder = createCompositeBuilder(job.getConfig());
        this.upgradedDocumentID = upgradedDocumentID;
    }

    /**
     * Returns if this job has upgraded it's ID scheme yet or not
     */
    public boolean isUpgradedDocumentID() {
        return upgradedDocumentID.get();
    }

    @Override
    protected String getJobId() {
        return job.getConfig().getId();
    }

    @Override
    protected void onStart(long now) {
        // this is needed to exclude buckets that can still receive new documents.
        DateHistogramGroupConfig dateHisto = job.getConfig().getGroupConfig().getDateHistogram();
        long rounded = dateHisto.createRounding().round(now);
        if (dateHisto.getDelay() != null) {
            // if the job has a delay we filter all documents that appear before it.
            maxBoundary = rounded - TimeValue.parseTimeValue(dateHisto.getDelay().toString(), "").millis();
        } else {
            maxBoundary = rounded;
        }
    }
    
    @Override
    protected SearchRequest buildSearchRequest() {
            // Indexer is single-threaded, and only place that the ID scheme can get upgraded is doSaveState(), so
            // we can pass down the boolean value rather than the atomic here
        final Map<String, Object> position = getPosition();
        SearchSourceBuilder searchSource = new SearchSourceBuilder()
                .size(0)
                .trackTotalHits(false)
                // make sure we always compute complete buckets that appears before the configured delay
                .query(createBoundaryQuery(position))
                .aggregation(compositeBuilder.aggregateAfter(position));
        return new SearchRequest(job.getConfig().getIndexPattern()).source(searchSource);
    }
    
    @Override
    protected Iteration<Map<String, Object>> doProcess(SearchResponse searchResponse) {
        final CompositeAggregation response = searchResponse.getAggregations().get(AGGREGATION_NAME);

        return new Iteration<>(
                IndexerUtils.processBuckets(response, job.getConfig().getRollupIndex(), getStats(),
                        job.getConfig().getGroupConfig(), job.getConfig().getId(), upgradedDocumentID.get()),
                response.afterKey(), response.getBuckets().isEmpty());
    }

    /**
     * Creates a skeleton {@link CompositeAggregationBuilder} from the provided job config.
     * @param config The config for the job.
     * @return The composite aggregation that creates the rollup buckets
     */
    private CompositeAggregationBuilder createCompositeBuilder(RollupJobConfig config) {
        final GroupConfig groupConfig = config.getGroupConfig();
        List<CompositeValuesSourceBuilder<?>> builders = new ArrayList<>();
        Map<String, Object> metadata = new HashMap<>();

        // Add all the agg builders to our request in order: date_histo -> histo -> terms
        if (groupConfig != null) {
            builders.addAll(groupConfig.getDateHistogram().toBuilders());
            metadata.putAll(groupConfig.getDateHistogram().getMetadata());
            if (groupConfig.getHistogram() != null) {
                builders.addAll(groupConfig.getHistogram().toBuilders());
                metadata.putAll(groupConfig.getHistogram().getMetadata());
            }
            if (groupConfig.getTerms() != null) {
                builders.addAll(groupConfig.getTerms().toBuilders());
            }
        }

        CompositeAggregationBuilder composite = new CompositeAggregationBuilder(AGGREGATION_NAME, builders);
        config.getMetricsConfig().forEach(m -> m.toBuilders().forEach(composite::subAggregation));
        if (metadata.isEmpty() == false) {
            composite.setMetaData(metadata);
        }
        composite.size(config.getPageSize());

        return composite;
    }

    /**
     * Creates the range query that limits the search to documents that appear before the maximum allowed time
     * (see {@link #maxBoundary}
     * and on or after the last processed time.
     * @param position The current position of the pagination
     * @return The range query to execute
     */
    private QueryBuilder createBoundaryQuery(Map<String, Object> position) {
        assert maxBoundary < Long.MAX_VALUE;
        DateHistogramGroupConfig dateHisto = job.getConfig().getGroupConfig().getDateHistogram();
        String fieldName = dateHisto.getField();
        String rollupFieldName = fieldName + "."  + DateHistogramAggregationBuilder.NAME;
        long lowerBound = 0L;
        if (position != null) {
            Number value = (Number) position.get(rollupFieldName);
            lowerBound = value.longValue();
        }
        assert lowerBound <= maxBoundary;
        final RangeQueryBuilder query = new RangeQueryBuilder(fieldName)
                .gte(lowerBound)
                .lt(maxBoundary)
                .format("epoch_millis");
        return query;
    }
}

