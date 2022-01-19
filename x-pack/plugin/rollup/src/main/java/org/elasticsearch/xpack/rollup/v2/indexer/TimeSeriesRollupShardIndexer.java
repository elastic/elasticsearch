/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rollup.v2.indexer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.CancellableThreads.ExecutionCancelledException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.DocCountProvider;
import org.elasticsearch.xpack.core.rollup.RollupActionConfig;
import org.elasticsearch.xpack.core.rollup.action.RollupShardStatus;
import org.elasticsearch.xpack.core.rollup.action.RollupShardStatus.Status;
import org.elasticsearch.xpack.rollup.v2.RollupShardIndexer;
import org.elasticsearch.xpack.rollup.v2.indexer.metrics.LeafMetricField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * rollup data in sort mode
 */
public class TimeSeriesRollupShardIndexer extends RollupShardIndexer {
    private static final Logger logger = LogManager.getLogger(TimeSeriesRollupShardIndexer.class);

    public TimeSeriesRollupShardIndexer(
        RollupShardStatus rollupShardStatus,
        Client client,
        IndexService indexService,
        ShardId shardId,
        RollupActionConfig config,
        String tmpIndex
    ) {
        super(rollupShardStatus, client, indexService, shardId, config, tmpIndex);
    }

    @Override
    public void execute() throws IOException {
        long start = System.currentTimeMillis();
        try (searcher; bulkProcessor) {
            TimeSeriesIndexSearcher timeSeriesIndexSearcher = new TimeSeriesIndexSearcher(searcher);
            TimeSeriesCollector timeSeriesCollector = new TimeSeriesCollector();
            timeSeriesCollector.preCollection();
            timeSeriesIndexSearcher.search(new MatchAllDocsQuery(), timeSeriesCollector);
            timeSeriesCollector.postCollection();
            bulkProcessor.flush();
        }

        if (status.getStatus() == Status.ABORT) {
            logger.warn(
                "[{}] rolling abort, sent [{}], indexed [{}], failed[{}]",
                indexShard.shardId(),
                numIndexed.get(),
                numIndexed.get(),
                numFailed.get()
            );
            throw new ExecutionCancelledException("[" + indexShard.shardId() + "] rollup cancelled");
        }

        logger.info(
            "sorted rollup execute [{}], cost [{}], Received [{}], Skip [{}], Successfully sent [{}], indexed [{}], failed[{}]",
            indexShard.shardId(),
            (System.currentTimeMillis() - start),
            numReceived.get(),
            numSkip.get(),
            numSent.get(),
            numIndexed.get(),
            numFailed.get()
        );
        status.setStatus(Status.STOP);
        return;
    }

    private class TimeSeriesCollector extends BucketCollector {
        final AtomicReference<BucketKey> currentKey = new AtomicReference<>();
        final AtomicInteger docCount = new AtomicInteger(0);
        final AtomicInteger keyCount = new AtomicInteger(0);
        final AtomicLong nextBucket = new AtomicLong(Long.MIN_VALUE);

        @Override
        public LeafBucketCollector getLeafCollector(LeafReaderContext ctx) throws IOException {
            final FormattedDocValues timestampField = timestampFetcher.getGroupLeaf(ctx);
            final FormattedDocValues[] groupFieldLeaf = leafGroupFetchers(ctx);
            final LeafMetricField[] metricsFieldLeaf = leafMetricFields(ctx);
            DocCountProvider docCountProvider = new DocCountProvider();
            docCountProvider.setLeafReaderContext(ctx);

            return new LeafBucketCollector() {
                @Override
                public void collect(int doc, long owningBucketOrd) throws IOException {
                    if (isCanceled()) {
                        // TODO check if this can throw cancel exception
                        return;
                    }

                    numReceived.incrementAndGet();
                    Long timestamp = null;
                    if (timestampField.advanceExact(doc)) {
                        Object obj = timestampField.nextValue();
                        if (obj instanceof Number == false) {
                            throw new IllegalArgumentException("Expected [Number], got [" + obj.getClass() + "]");
                        }
                        timestamp = ((Number) obj).longValue();
                    }

                    if (timestamp == null) {
                        logger.trace("[{}] timestamp missing, bucket ord [{}], docId [{}]", indexShard.shardId(), owningBucketOrd, doc);
                        numSkip.incrementAndGet();
                        return;
                    }

                    List<Object> groupFields = new ArrayList<>();
                    for (FormattedDocValues leafField : groupFieldLeaf) {
                        if (leafField.advanceExact(doc)) {
                            groupFields.add(leafField.nextValue());
                        }
                    }

                    if (currentKey.get() != null
                        && (false == Objects.equals(currentKey.get().getGroupFields(), groupFields)
                            || timestamp >= nextBucket.get()
                            || timestamp < currentKey.get().getTimestamp())) {
                        indexBucket(currentKey.get(), docCount.get());
                        keyCount.incrementAndGet();
                        // reset
                        currentKey.set(null);
                        docCount.set(0);
                        resetMetricCollectors();
                    }

                    if (currentKey.get() == null) {
                        Long currentBucket = rounding.round(timestamp);
                        nextBucket.set(rounding.nextRoundingValue(currentBucket));
                        currentKey.set(new BucketKey(currentBucket, groupFields));
                    }

                    docCount.addAndGet(docCountProvider.getDocCount(doc));
                    for (LeafMetricField metricField : metricsFieldLeaf) {
                        metricField.collectMetric(doc);
                    }
                }
            };
        }

        @Override
        public void preCollection() throws IOException {

        }

        @Override
        public void postCollection() throws IOException {
            if (currentKey.get() != null) {
                indexBucket(currentKey.get(), docCount.get());
                keyCount.incrementAndGet();
            }
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
        }
    }
}
