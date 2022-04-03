/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup.v2;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.DocCountFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.DocCountProvider;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.timeseries.TimeSeriesIndexSearcher;
import org.elasticsearch.xpack.core.rollup.RollupActionConfig;
import org.elasticsearch.xpack.core.rollup.RollupActionDateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;

import java.io.Closeable;
import java.io.IOException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * An indexer for rollups that iterates documents collected by {@link TimeSeriesIndexSearcher},
 * computes the rollup buckets and stores the buckets in the rollup index.
 *
 * The documents collected by the {@link TimeSeriesIndexSearcher} are expected to be sorted
 * by _tsid in ascending order and @timestamp in descending order.
 */
class RollupShardIndexer {
    private static final Logger logger = LogManager.getLogger(RollupShardIndexer.class);

    private final IndexShard indexShard;
    private final Client client;
    private final RollupActionConfig config;
    private final String rollupIndex;

    private final Engine.Searcher searcher;
    private final SearchExecutionContext searchExecutionContext;
    private final MappedFieldType timestampField;
    private final DocValueFormat timestampFormat;
    private final Rounding.Prepared rounding;

    private final List<FieldValueFetcher> dimensionFieldFetchers;
    private final List<FieldValueFetcher> metricFieldFetchers;

    private final AtomicLong numSent = new AtomicLong();
    private final AtomicLong numIndexed = new AtomicLong();
    private final AtomicLong numFailed = new AtomicLong();

    RollupShardIndexer(Client client, IndexService indexService, ShardId shardId, RollupActionConfig config, String rollupIndex) {
        this.client = client;
        this.indexShard = indexService.getShard(shardId.id());
        this.config = config;
        this.rollupIndex = rollupIndex;

        this.searcher = indexShard.acquireSearcher("rollup");
        Closeable toClose = searcher;
        try {
            this.searchExecutionContext = indexService.newSearchExecutionContext(
                indexShard.shardId().id(),
                0,
                searcher,
                () -> 0L,
                null,
                Collections.emptyMap()
            );
            this.timestampField = searchExecutionContext.getFieldType(DataStreamTimestampFieldMapper.DEFAULT_PATH);
            this.timestampFormat = timestampField.docValueFormat(null, null);
            this.rounding = createRounding(config.getGroupConfig().getDateHistogram()).prepareForUnknown();

            // TODO: Replace this config parsing with index mapping parsing
            if (config.getGroupConfig().getTerms() != null && config.getGroupConfig().getTerms().getFields().length > 0) {
                final String[] dimensionFields = config.getGroupConfig().getTerms().getFields();
                this.dimensionFieldFetchers = FieldValueFetcher.build(searchExecutionContext, dimensionFields);
            } else {
                this.dimensionFieldFetchers = Collections.emptyList();
            }

            if (config.getMetricsConfig().size() > 0) {
                final String[] metricFields = config.getMetricsConfig().stream().map(MetricConfig::getField).toArray(String[]::new);
                this.metricFieldFetchers = FieldValueFetcher.build(searchExecutionContext, metricFields);
            } else {
                this.metricFieldFetchers = Collections.emptyList();
            }

            toClose = null;
        } finally {
            IOUtils.closeWhileHandlingException(toClose);
        }
    }

    public long execute() throws IOException {
        BulkProcessor bulkProcessor = createBulkProcessor();
        try (searcher; bulkProcessor) {
            // TODO: add cancellations
            final TimeSeriesIndexSearcher timeSeriesSearcher = new TimeSeriesIndexSearcher(searcher, List.of());
            TimeSeriesBucketCollector bucketCollector = new TimeSeriesBucketCollector(bulkProcessor);
            bucketCollector.preCollection();
            timeSeriesSearcher.search(new MatchAllDocsQuery(), bucketCollector);
            bucketCollector.postCollection();
        }
        // TODO: check that numIndexed == numSent, otherwise throw an exception
        logger.info(
            "Shard {} successfully sent [{}], indexed [{}], failed [{}]",
            indexShard.shardId(),
            numSent.get(),
            numIndexed.get(),
            numFailed.get()
        );
        return numIndexed.get();
    }

    private BulkProcessor createBulkProcessor() {
        final BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                numSent.addAndGet(request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                numIndexed.addAndGet(request.numberOfActions());
                if (response.hasFailures()) {
                    Map<String, String> failures = Arrays.stream(response.getItems())
                        .filter(BulkItemResponse::isFailed)
                        .collect(
                            Collectors.toMap(
                                BulkItemResponse::getId,
                                BulkItemResponse::getFailureMessage,
                                (msg1, msg2) -> Objects.equals(msg1, msg2) ? msg1 : msg1 + "," + msg2
                            )
                        );
                    numFailed.addAndGet(failures.size());
                    logger.error("Shard {} failed to populate rollup index: [{}]", indexShard.shardId(), failures);
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                if (failure != null) {
                    long items = request.numberOfActions();
                    numSent.addAndGet(-items);
                    numFailed.addAndGet(items);
                }
            }
        };
        return BulkProcessor.builder(client::bulk, listener, "rollup-shard-indexer")
            .setBulkActions(10000)
            .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.MB))
            // execute the bulk request on the same thread
            .setConcurrentRequests(0)
            .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(1000), 3))
            .build();
    }

    private static Rounding createRounding(RollupActionDateHistogramGroupConfig groupConfig) {
        DateHistogramInterval interval = groupConfig.getInterval();
        ZoneId zoneId = groupConfig.getTimeZone() != null ? ZoneId.of(groupConfig.getTimeZone()) : null;
        Rounding.Builder tzRoundingBuilder;
        if (groupConfig instanceof RollupActionDateHistogramGroupConfig.FixedInterval) {
            TimeValue timeValue = TimeValue.parseTimeValue(
                interval.toString(),
                null,
                RollupShardIndexer.class.getSimpleName() + ".interval"
            );
            tzRoundingBuilder = Rounding.builder(timeValue);
        } else if (groupConfig instanceof RollupActionDateHistogramGroupConfig.CalendarInterval) {
            Rounding.DateTimeUnit dateTimeUnit = DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(interval.toString());
            tzRoundingBuilder = Rounding.builder(dateTimeUnit);
        } else {
            throw new IllegalStateException("unsupported interval type");
        }
        return tzRoundingBuilder.timeZone(zoneId).build();
    }

    private class TimeSeriesBucketCollector extends BucketCollector {
        private final BulkProcessor bulkProcessor;
        private long docsProcessed;
        private long bucketsCreated;
        private final RollupBucketBuilder rollupBucketBuilder = new RollupBucketBuilder();
        long lastTimestamp = Long.MAX_VALUE;
        BytesRef lastTsid = null;

        TimeSeriesBucketCollector(BulkProcessor bulkProcessor) {
            this.bulkProcessor = bulkProcessor;
        }

        @Override
        public LeafBucketCollector getLeafCollector(final AggregationExecutionContext aggCtx) throws IOException {
            final LeafReaderContext ctx = aggCtx.getLeafReaderContext();
            final SortedNumericDocValues timestampValues = DocValues.getSortedNumeric(ctx.reader(), timestampField.name());
            final DocCountProvider docCountProvider = new DocCountProvider();
            docCountProvider.setLeafReaderContext(ctx);
            final Map<String, FormattedDocValues> metricsFieldLeaves = new HashMap<>();
            for (FieldValueFetcher fetcher : metricFieldFetchers) {
                FormattedDocValues leafField = fetcher.getLeaf(ctx);
                metricsFieldLeaves.put(fetcher.name, leafField);
            }

            return new LeafBucketCollector() {
                @Override
                public void collect(int docId, long owningBucketOrd) throws IOException {
                    BytesRef tsid = aggCtx.getTsid();
                    if (tsid == null || timestampValues.advanceExact(docId) == false) {
                        throw new IllegalArgumentException(
                            "Document without [" + TimeSeriesIdFieldMapper.NAME + "] or [" + timestampField.name() + "] field was found."
                        );
                    }
                    assert timestampValues.docValueCount() == 1 : "@timestamp field cannot be a multi-value field";
                    long timestamp = timestampValues.nextValue();
                    long histoTimestamp = rounding.round(timestamp);

                    logger.trace(
                        "Doc: [{}] - _tsid: [{}], @timestamp: [{}}] -> rollup bucket ts: [{}]",
                        docId,
                        DocValueFormat.TIME_SERIES_ID.format(tsid),
                        timestampFormat.format(timestamp),
                        timestampFormat.format(histoTimestamp)
                    );

                    /*
                     * Sanity checks to ensure that we receive documents in the correct order
                     * - _tsid must be sorted in ascending order
                     * - @timestamp must be sorted in descending order within the same _tsid
                     */
                    assert lastTsid == null || lastTsid.compareTo(tsid) <= 0
                        : "_tsid is not sorted in ascending order: ["
                            + DocValueFormat.TIME_SERIES_ID.format(lastTsid)
                            + "] -> ["
                            + DocValueFormat.TIME_SERIES_ID.format(tsid)
                            + "]";
                    assert tsid.equals(lastTsid) == false || lastTimestamp >= timestamp
                        : "@timestamp is not sorted in descending order: ["
                            + timestampFormat.format(lastTimestamp)
                            + "] -> ["
                            + timestampFormat.format(timestamp)
                            + "]";
                    lastTsid = BytesRef.deepCopyOf(tsid);
                    lastTimestamp = timestamp;

                    if (tsid.equals(rollupBucketBuilder.tsid()) == false || rollupBucketBuilder.timestamp() != histoTimestamp) {
                        // Flush rollup doc if not empty
                        if (rollupBucketBuilder.isEmpty() == false) {
                            Map<String, Object> doc = rollupBucketBuilder.buildRollupDocument();
                            indexBucket(doc);
                        }

                        // Create new rollup bucket
                        rollupBucketBuilder.init(tsid, histoTimestamp);
                        bucketsCreated++;
                    }

                    int docCount = docCountProvider.getDocCount(docId);
                    rollupBucketBuilder.collectDocCount(docCount);

                    for (Map.Entry<String, FormattedDocValues> e : metricsFieldLeaves.entrySet()) {
                        String fieldName = e.getKey();
                        FormattedDocValues leafField = e.getValue();

                        if (leafField.advanceExact(docId)) {
                            for (int i = 0; i < leafField.docValueCount(); i++) {
                                Object obj = leafField.nextValue();
                                if (obj instanceof Number number) {
                                    // Collect docs to rollup doc
                                    double value = number.doubleValue();
                                    rollupBucketBuilder.collectMetric(fieldName, value);
                                    // TODO: Implement aggregate_metric_double for rollup of rollups
                                } else {
                                    throw new IllegalArgumentException("Expected [Number], got [" + obj.getClass() + "]");
                                }
                            }
                        }
                    }
                    docsProcessed++;
                }
            };
        }

        private void indexBucket(Map<String, Object> doc) {
            IndexRequestBuilder request = client.prepareIndex(rollupIndex);
            request.setSource(doc);
            logger.trace("Indexing rollup doc: [{}]", doc);
            bulkProcessor.add(request.request());
        }

        @Override
        public void preCollection() throws IOException {
            // no-op
        }

        @Override
        public void postCollection() throws IOException {
            // Flush rollup doc if not empty
            if (rollupBucketBuilder.isEmpty() == false) {
                Map<String, Object> doc = rollupBucketBuilder.buildRollupDocument();
                indexBucket(doc);
            }
            bulkProcessor.flush();
            logger.info("Shard {} processed [{}] docs, created [{}] rollup buckets", indexShard.shardId(), docsProcessed, bucketsCreated);
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
        }
    }

    private class RollupBucketBuilder {
        private BytesRef tsid;
        private long timestamp;
        private int docCount;
        private final Map<String, MetricFieldProducer> metricFields;

        RollupBucketBuilder() {
            this.metricFields = MetricFieldProducer.buildMetrics(config.getMetricsConfig());
        }

        public RollupBucketBuilder init(BytesRef tsid, long timestamp) {
            this.tsid = BytesRef.deepCopyOf(tsid);
            this.timestamp = timestamp;
            this.docCount = 0;
            this.metricFields.values().stream().forEach(p -> p.reset());
            logger.trace(
                "New bucket for _tsid: [{}], @timestamp: [{}]",
                DocValueFormat.TIME_SERIES_ID.format(tsid),
                timestampFormat.format(timestamp)
            );

            return this;
        }

        public void collectMetric(String fieldName, double value) {
            MetricFieldProducer field = this.metricFields.get(fieldName);
            for (MetricFieldProducer.Metric metric : field.metrics) {
                metric.collect(value);
            }
        }

        public void collectDocCount(int docCount) {
            this.docCount += docCount;
        }

        public Map<String, Object> buildRollupDocument() {
            if (tsid == null || timestamp == 0) {
                throw new IllegalStateException("Rollup bucket builder is not initialized.");
            }

            // Extract dimension values from _tsid field, so we avoid load them from doc_values
            @SuppressWarnings("unchecked")
            Map<String, Object> dimensions = (Map<String, Object>) DocValueFormat.TIME_SERIES_ID.format(tsid);

            Map<String, Object> doc = Maps.newMapWithExpectedSize(2 + dimensions.size() + metricFields.size());
            doc.put(DocCountFieldMapper.NAME, docCount);
            doc.put(timestampField.name(), timestampFormat.format(timestamp));

            for (FieldValueFetcher fetcher : dimensionFieldFetchers) {
                Object value = dimensions.get(fetcher.name);
                if (value != null) {
                    doc.put(fetcher.name, fetcher.format(value));
                }
            }

            for (MetricFieldProducer field : metricFields.values()) {
                Map<String, Object> map = new HashMap<>();
                for (MetricFieldProducer.Metric metric : field.metrics) {
                    map.put(metric.name, metric.get());
                }
                doc.put(field.fieldName, map);
            }

            return doc;
        }

        public long timestamp() {
            return timestamp;
        }

        public BytesRef tsid() {
            return tsid;
        }

        public int docCount() {
            return docCount;
        }

        public boolean isEmpty() {
            return docCount() == 0;
        }
    }
}
