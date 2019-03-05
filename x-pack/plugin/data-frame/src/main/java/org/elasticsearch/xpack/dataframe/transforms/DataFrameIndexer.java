/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerTransformStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.indexing.AsyncTwoPhaseIndexer;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.indexing.IterationResult;
import org.elasticsearch.xpack.dataframe.transforms.pivot.Pivot;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public abstract class DataFrameIndexer extends AsyncTwoPhaseIndexer<Map<String, Object>, DataFrameIndexerTransformStats> {

    public static final String COMPOSITE_AGGREGATION_NAME = "_data_frame";
    private static final Logger logger = LogManager.getLogger(DataFrameIndexer.class);

    private Pivot pivot;

    public DataFrameIndexer(Executor executor, AtomicReference<IndexerState> initialState, Map<String, Object> initialPosition) {
        super(executor, initialState, initialPosition, new DataFrameIndexerTransformStats());
    }

    protected abstract DataFrameTransformConfig getConfig();

    @Override
    protected void onStartJob(long now) {
        QueryBuilder queryBuilder = getConfig().getQueryConfig().getQuery();

        pivot = new Pivot(getConfig().getSource(), queryBuilder, getConfig().getPivotConfig());
    }

    @Override
    protected IterationResult<Map<String, Object>> doProcess(SearchResponse searchResponse) {
        final CompositeAggregation agg = searchResponse.getAggregations().get(COMPOSITE_AGGREGATION_NAME);
        return new IterationResult<>(processBucketsToIndexRequests(agg).collect(Collectors.toList()), agg.afterKey(),
                agg.getBuckets().isEmpty());
    }

    /*
     * Parses the result and creates a stream of indexable documents
     *
     * Implementation decisions:
     *
     * Extraction uses generic maps as intermediate exchange format in order to hook in ingest pipelines/processors
     * in later versions, see {@link IngestDocument).
     */
    private Stream<IndexRequest> processBucketsToIndexRequests(CompositeAggregation agg) {
        final DataFrameTransformConfig transformConfig = getConfig();
        String indexName = transformConfig.getDestination();

        return pivot.extractResults(agg, getStats()).map(document -> {
            XContentBuilder builder;
            try {
                builder = jsonBuilder();
                builder.map(document);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            IndexRequest request = new IndexRequest(indexName).source(builder);
            return request;
        });
    }

    @Override
    protected SearchRequest buildSearchRequest() {
        return pivot.buildSearchRequest(getPosition());
    }
}
