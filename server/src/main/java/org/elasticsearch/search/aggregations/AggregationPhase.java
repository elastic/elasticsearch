/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FilterLeafCollector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.search.aggregations.support.TimeSeriesIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.query.QueryPhase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Aggregation phase of a search request, used to collect aggregations
 */
public class AggregationPhase {

    private AggregationPhase() {}

    public static void preProcess(SearchContext context) {
        if (context.aggregations() == null) {
            return;
        }
        final Supplier<AggregatorCollector> collectorSupplier;
        if (context.aggregations().isInSortOrderExecutionRequired()) {
            AggregatorCollector collector = newAggregatorCollector(context);
            executeInSortOrder(context, collector.bucketCollector);
            collectorSupplier = () -> new AggregatorCollector(collector.aggregators, BucketCollector.NO_OP_BUCKET_COLLECTOR);
        } else {
            collectorSupplier = () -> newAggregatorCollector(context);
        }
        context.aggregations()
            .registerAggsCollectorManager(
                new AggregatorCollectorManager(
                    collectorSupplier,
                    internalAggregations -> context.queryResult().aggregations(internalAggregations),
                    () -> context.aggregations().getAggregationReduceContextBuilder().forPartialReduction()
                )
            );
    }

    private static AggregatorCollector newAggregatorCollector(SearchContext context) {
        try {
            Aggregator[] aggregators = context.aggregations().factories().createTopLevelAggregators();
            BucketCollector bucketCollector = MultiBucketCollector.wrap(true, List.of(aggregators));
            bucketCollector.preCollection();
            return new AggregatorCollector(aggregators, bucketCollector);
        } catch (IOException e) {
            throw new AggregationInitializationException("Could not initialize aggregators", e);
        }
    }

    private static void executeInSortOrder(SearchContext context, BucketCollector collector) {
        try {
            if (context.searcher().getLeafContexts().size() == 1) {
                searchSingleLeaf(context, collector);
            } else {
                TimeSeriesIndexSearcher searcher = new TimeSeriesIndexSearcher(context.searcher(), getCancellationChecks(context));
                searcher.setMinimumScore(context.minimumScore());
                searcher.setProfiler(context);
                searcher.search(context.rewrittenQuery(), collector);
            }
        } catch (IOException e) {
            // Seems like this should be 400 (non-retryable), but we clearly intentionally throw a 500 here. Why?
            throw new AggregationExecutionException("Could not perform time series aggregation", e);
        }
    }

    private static void searchSingleLeaf(SearchContext context, BucketCollector collector) throws IOException {
        context.searcher().search(context.rewrittenQuery(), new Collector() {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                SortedNumericDocValues timestampField = DocValues.getSortedNumeric(context.reader(), DataStream.TIMESTAMP_FIELD_NAME);
                SortedDocValues tsidField = DocValues.getSorted(context.reader(), TimeSeriesIdFieldMapper.NAME);
                Bits liveDocs = context.reader().getLiveDocs();

                BytesRef[] tsid = new BytesRef[1];
                int[] tsidOrd = new int[1];
                long[] timestamp = new long[1];
                LeafCollector inner = collector.getLeafCollector(
                    new AggregationExecutionContext(context, () -> tsid[0], () -> timestamp[0], () -> tsidOrd[0])
                );

                return new FilterLeafCollector(inner) {
                    @Override
                    public void collect(int doc) throws IOException {
                        if (tsidField.advanceExact(doc) && timestampField.advanceExact(doc) && (liveDocs == null || liveDocs.get(doc))) {
                            tsidOrd[0] = tsidField.ordValue();
                            tsid[0] = tsidField.lookupOrd(tsidOrd[0]);
                            timestamp[0] = timestampField.nextValue();
                            inner.collect(doc);
                        }
                    }
                };
            }

            @Override
            public ScoreMode scoreMode() {
                return collector.scoreMode();
            }
        });
        collector.postCollection();
    }

    private static List<Runnable> getCancellationChecks(SearchContext context) {
        List<Runnable> cancellationChecks = new ArrayList<>();
        if (context.lowLevelCancellation()) {
            // This searching doesn't live beyond this phase, so we don't need to remove query cancellation
            cancellationChecks.add(() -> {
                final SearchShardTask task = context.getTask();
                if (task != null) {
                    task.ensureNotCancelled();
                }
            });
        }

        final Runnable timeoutRunnable = QueryPhase.getTimeoutCheck(context);
        if (timeoutRunnable != null) {
            cancellationChecks.add(timeoutRunnable);
        }

        return cancellationChecks;
    }
}
