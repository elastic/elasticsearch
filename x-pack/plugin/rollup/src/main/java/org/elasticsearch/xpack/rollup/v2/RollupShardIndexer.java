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
import org.apache.lucene.index.SortedDocValues;
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
import org.elasticsearch.index.mapper.DateFieldMapper;
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
import org.elasticsearch.xpack.core.rollup.job.HistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.elasticsearch.xpack.core.rollup.job.TermsGroupConfig;

import java.io.Closeable;
import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * An indexer for rollup that sorts the buckets from the provided source shard on disk and send them
 * to the target rollup index.
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
            this.timestampField = searchExecutionContext.getFieldType(config.getGroupConfig().getDateHistogram().getField());
            verifyTimestampField(timestampField);
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

    private void verifyTimestampField(MappedFieldType fieldType) {
        if (fieldType == null) {
            throw new IllegalArgumentException("Timestamp field type is null");
        }
        // TODO: Support nanosecond fields?
        if (fieldType instanceof DateFieldMapper.DateFieldType == false) {
            throw new IllegalArgumentException("Wrong type for the timestamp field, " + "expected [date], got [" + fieldType.name() + "]");
        }
        if (fieldType.isIndexed() == false) {
            throw new IllegalArgumentException("The timestamp field [" + fieldType.name() + "] is not indexed");
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
        logger.info("Successfully sent [" + numSent.get() + "], indexed [" + numIndexed.get() + "]");
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
                    logger.error("failures: [{}]", failures);
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                long items = request.numberOfActions();
                numSent.addAndGet(-items);
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

        private final RollupBucketBuilder rollupBucketBuilder = new RollupBucketBuilder();
        private final BulkProcessor bulkProcessor;
        private long docsProcessed = 0;
        private long bucketsCreated = 0;

        TimeSeriesBucketCollector(BulkProcessor bulkProcessor) {
            this.bulkProcessor = bulkProcessor;
        }

        @Override
        public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx) throws IOException {
            LeafReaderContext ctx = aggCtx.getLeafReaderContext();
            final SortedDocValues tsidValues = DocValues.getSorted(ctx.reader(), TimeSeriesIdFieldMapper.NAME);
            final SortedNumericDocValues timestampValues = DocValues.getSortedNumeric(ctx.reader(), timestampField.name());

            rollupBucketBuilder.setLeafReaderContext(ctx);

            return new LeafBucketCollector() {
                @Override
                public void collect(int docId, long owningBucketOrd) throws IOException {
                    if (tsidValues.advanceExact(docId) && timestampValues.advanceExact(docId)) {
                        BytesRef tsid = tsidValues.lookupOrd(tsidValues.ordValue());
                        long timestamp = timestampValues.nextValue();
                        long bucketTimestamp = rounding.round(timestamp);

                        if (tsid.equals(rollupBucketBuilder.tsid()) == false || rollupBucketBuilder.timestamp() != bucketTimestamp) {
                            // Flush rollup doc if not empty
                            if (rollupBucketBuilder.tsid() != null) {
                                Map<String, Object> doc = rollupBucketBuilder.buildRollupDocument();
                                indexBucket(doc);
                            }

                            // Create new rollup bucket
                            rollupBucketBuilder.init(tsid, bucketTimestamp);
                            bucketsCreated++;
                        }

                        // Collect docs to rollup doc
                        rollupBucketBuilder.addDocument(docId);
                    }
                }
            };
        }

        private void indexBucket(Map<String, Object> doc) {
            IndexRequestBuilder request = client.prepareIndex(rollupIndex);
            request.setSource(doc);
            bulkProcessor.add(request.request());
        }

        @Override
        public void preCollection() throws IOException {
            // no-op
        }

        @Override
        public void postCollection() throws IOException {
            // Flush rollup doc if not empty
            if (rollupBucketBuilder.tsid() != null) {
                Map<String, Object> doc = rollupBucketBuilder.buildRollupDocument();
                indexBucket(doc);
            }
            bulkProcessor.flush();
            logger.info("Docs processed: " + docsProcessed + ", buckets created: " + bucketsCreated);
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

        private LeafReaderContext ctx;
        private DocCountProvider docCountProvider;

        private Map<String, MetricFieldProducer> metricFields;

        RollupBucketBuilder() {
            docCountProvider = new DocCountProvider();
        }

        public void setLeafReaderContext(LeafReaderContext ctx) throws IOException {
            this.ctx = ctx;
            docCountProvider.setLeafReaderContext(ctx);
        }

        public RollupBucketBuilder init(BytesRef tsid, long timestamp) {
            this.tsid = BytesRef.deepCopyOf(tsid);
            ;
            this.timestamp = timestamp;
            this.docCount = 0;
            metricFields = MetricFieldProducer.buildMetrics(config.getMetricsConfig());
            return this;
        }

        public void addDocument(int docId) throws IOException {
            /* Skip loading dimensions, we decode them from tsid directly
            // We extract dimension values only once per rollup bucket
            if (docCount == 0) {
                collectDimensions(docId);
            }
            */
            collectMetrics(docId);

            // Compute doc_count for bucket
            int docCount = docCountProvider.getDocCount(docId);
            this.docCount += docCount;
        }

        // TODO: Remove this method, because we don't need to load the doc_values.
        // We can parse _tsid instead
        private void collectDimensions(int docId) throws IOException {
            for (FieldValueFetcher f : dimensionFieldFetchers) {
                FormattedDocValues leafField = f.getLeaf(ctx);
                if (leafField.advanceExact(docId)) {
                    List<Object> lst = new ArrayList<>();
                    for (int i = 0; i < leafField.docValueCount(); i++) {
                        lst.add(leafField.nextValue());
                    }
                    // combinationKeys.add(lst);
                }
            }
        }

        private void collectMetrics(int docId) throws IOException {
            for (FieldValueFetcher fetcher : metricFieldFetchers) {
                FormattedDocValues formattedDocValues = fetcher.getLeaf(ctx);

                if (formattedDocValues.advanceExact(docId)) {
                    for (int i = 0; i < formattedDocValues.docValueCount(); i++) {
                        Object obj = formattedDocValues.nextValue();
                        if (obj instanceof Number number) {
                            MetricFieldProducer field = metricFields.get(fetcher.name);
                            double value = number.doubleValue();
                            for (MetricFieldProducer.Metric metric : field.metrics) {
                                metric.collect(value);
                            }
                        } else {
                            throw new IllegalArgumentException("Expected [Number], got [" + obj.getClass() + "]");
                        }
                    }
                }
            }
        }

        public Map<String, Object> buildRollupDocument() {
            if (tsid == null || timestamp == 0) {
                throw new IllegalStateException("Rollup bucket builder is not initialized.");
            }

            // Extract dimension values from tsid, so we avoid load them from doc_values
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
    }
}
