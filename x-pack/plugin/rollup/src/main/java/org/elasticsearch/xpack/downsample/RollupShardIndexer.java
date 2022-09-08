/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.downsample;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
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
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
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
import org.elasticsearch.search.aggregations.timeseries.TimeSeriesIndexSearcher;
import org.elasticsearch.xpack.core.downsample.DownsampleConfig;
import org.elasticsearch.xpack.core.downsample.RollupIndexerAction;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.core.Strings.format;

/**
 * An indexer for rollups that iterates documents collected by {@link TimeSeriesIndexSearcher},
 * computes the rollup buckets and stores the buckets in the rollup index.
 *
 * The documents collected by the {@link TimeSeriesIndexSearcher} are expected to be sorted
 * by _tsid in ascending order and @timestamp in descending order.
 */
class RollupShardIndexer {
    private static final Logger logger = LogManager.getLogger(RollupShardIndexer.class);
    public static final int ROLLUP_BULK_ACTIONS = 10000;
    public static final ByteSizeValue ROLLUP_BULK_SIZE = new ByteSizeValue(1, ByteSizeUnit.MB);

    private final IndexShard indexShard;
    private final Client client;
    private final DownsampleConfig config;
    private final String rollupIndex;

    private final Engine.Searcher searcher;
    private final SearchExecutionContext searchExecutionContext;
    private final MappedFieldType timestampField;
    private final DocValueFormat timestampFormat;
    private final Rounding.Prepared rounding;

    private final String[] dimensionFields;
    private final String[] metricFields;
    private final String[] labelFields;
    private final List<FieldValueFetcher> metricFieldFetchers;
    private final List<FieldValueFetcher> labelFieldFetchers;

    private final AtomicLong numSent = new AtomicLong();
    private final AtomicLong numIndexed = new AtomicLong();
    private final AtomicLong numFailed = new AtomicLong();

    RollupShardIndexer(
        Client client,
        IndexService indexService,
        ShardId shardId,
        String rollupIndex,
        DownsampleConfig config,
        String[] dimensionFields,
        String[] metricFields,
        String[] labelFields
    ) {
        this.client = client;
        this.indexShard = indexService.getShard(shardId.id());
        this.config = config;
        this.rollupIndex = rollupIndex;
        this.dimensionFields = dimensionFields;
        this.metricFields = metricFields;
        this.labelFields = labelFields;

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
            this.rounding = config.createRounding();
            this.metricFieldFetchers = FieldValueFetcher.forMetrics(searchExecutionContext, metricFields);
            this.labelFieldFetchers = FieldValueFetcher.forLabels(searchExecutionContext, labelFields);
            toClose = null;
        } finally {
            IOUtils.closeWhileHandlingException(toClose);
        }
    }

    public RollupIndexerAction.ShardRollupResponse execute() throws IOException {
        long startTime = System.currentTimeMillis();
        BulkProcessor bulkProcessor = createBulkProcessor();
        try (searcher; bulkProcessor) {
            // TODO: add cancellations
            final TimeSeriesIndexSearcher timeSeriesSearcher = new TimeSeriesIndexSearcher(searcher, List.of());
            TimeSeriesBucketCollector bucketCollector = new TimeSeriesBucketCollector(bulkProcessor);
            bucketCollector.preCollection();
            timeSeriesSearcher.search(new MatchAllDocsQuery(), bucketCollector);
            bucketCollector.postCollection();
        }

        logger.info(
            "Shard [{}] successfully sent [{}], indexed [{}], failed [{}], took [{}]",
            indexShard.shardId(),
            numSent.get(),
            numIndexed.get(),
            numFailed.get(),
            TimeValue.timeValueMillis(System.currentTimeMillis() - startTime)
        );

        if (numIndexed.get() != numSent.get()) {
            throw new ElasticsearchException(
                "Shard ["
                    + indexShard.shardId()
                    + "] failed to index all rollup documents. Sent ["
                    + numSent.get()
                    + "], indexed ["
                    + numIndexed.get()
                    + "]."
            );
        }
        return new RollupIndexerAction.ShardRollupResponse(indexShard.shardId(), numIndexed.get());
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
                    logger.error("Shard [{}] failed to populate rollup index. Failures: [{}]", indexShard.shardId(), failures);
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                if (failure != null) {
                    long items = request.numberOfActions();
                    numFailed.addAndGet(items);
                    logger.error(() -> format("Shard [%s] failed to populate rollup index.", indexShard.shardId()), failure);
                }
            }
        };

        return BulkProcessor.builder(client::bulk, listener, "rollup-shard-indexer")
            .setBulkActions(ROLLUP_BULK_ACTIONS)
            .setBulkSize(ROLLUP_BULK_SIZE)
            // execute the bulk request on the same thread
            .setConcurrentRequests(0)
            .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(1000), 3))
            .build();
    }

    private class TimeSeriesBucketCollector extends BucketCollector {
        private final BulkProcessor bulkProcessor;
        private long docsProcessed;
        private long bucketsCreated;
        private final RollupBucketBuilder rollupBucketBuilder = new RollupBucketBuilder();
        long lastTimestamp = Long.MAX_VALUE;
        long lastHistoTimestamp = Long.MAX_VALUE;

        TimeSeriesBucketCollector(BulkProcessor bulkProcessor) {
            this.bulkProcessor = bulkProcessor;
        }

        @Override
        public LeafBucketCollector getLeafCollector(final AggregationExecutionContext aggCtx) throws IOException {
            final LeafReaderContext ctx = aggCtx.getLeafReaderContext();
            final DocCountProvider docCountProvider = new DocCountProvider();
            docCountProvider.setLeafReaderContext(ctx);
            final Map<String, FormattedDocValues> metricsFieldLeaves = new HashMap<>();
            for (FieldValueFetcher fetcher : metricFieldFetchers) {
                metricsFieldLeaves.put(fetcher.name(), fetcher.getLeaf(ctx));
            }

            final Map<String, FormattedDocValues> labelFieldLeaves = new HashMap<>();
            for (FieldValueFetcher fetcher : labelFieldFetchers) {
                labelFieldLeaves.put(fetcher.name(), fetcher.getLeaf(ctx));
            }

            Set<Map.Entry<String, FormattedDocValues>> fieldFetchers = Sets.union(
                metricsFieldLeaves.entrySet(),
                labelFieldLeaves.entrySet()
            );

            return new LeafBucketCollector() {
                @Override
                public void collect(int docId, long owningBucketOrd) throws IOException {
                    final BytesRef tsid = aggCtx.getTsid();
                    assert tsid != null : "Document without [" + TimeSeriesIdFieldMapper.NAME + "] field was found.";
                    final long timestamp = aggCtx.getTimestamp();

                    boolean tsidChanged = tsid.equals(rollupBucketBuilder.tsid()) == false;
                    if (tsidChanged || timestamp < lastHistoTimestamp) {
                        lastHistoTimestamp = Math.max(
                            rounding.round(timestamp),
                            searchExecutionContext.getIndexSettings().getTimestampBounds().startTime()
                        );
                    }

                    if (logger.isTraceEnabled()) {
                        logger.trace(
                            "Doc: [{}] - _tsid: [{}], @timestamp: [{}}] -> rollup bucket ts: [{}]",
                            docId,
                            DocValueFormat.TIME_SERIES_ID.format(tsid),
                            timestampFormat.format(timestamp),
                            timestampFormat.format(lastHistoTimestamp)
                        );
                    }

                    /*
                     * Sanity checks to ensure that we receive documents in the correct order
                     * - _tsid must be sorted in ascending order
                     * - @timestamp must be sorted in descending order within the same _tsid
                     */
                    BytesRef lastTsid = rollupBucketBuilder.tsid();
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
                    lastTimestamp = timestamp;

                    if (tsidChanged || rollupBucketBuilder.timestamp() != lastHistoTimestamp) {
                        // Flush rollup doc if not empty
                        if (rollupBucketBuilder.isEmpty() == false) {
                            Map<String, Object> doc = rollupBucketBuilder.buildRollupDocument();
                            indexBucket(doc);
                        }

                        // Create new rollup bucket
                        if (tsidChanged) {
                            rollupBucketBuilder.resetTsid(tsid, lastHistoTimestamp);
                        } else {
                            rollupBucketBuilder.resetTimestamp(lastHistoTimestamp);
                        }

                        bucketsCreated++;
                    }

                    final int docCount = docCountProvider.getDocCount(docId);
                    rollupBucketBuilder.collectDocCount(docCount);
                    for (Map.Entry<String, FormattedDocValues> e : fieldFetchers) {
                        final String fieldName = e.getKey();
                        final FormattedDocValues leafField = e.getValue();

                        if (leafField.advanceExact(docId)) {
                            rollupBucketBuilder.collect(fieldName, leafField.docValueCount(), docValueCount -> {
                                final Object[] values = new Object[docValueCount];
                                for (int i = 0; i < docValueCount; ++i) {
                                    try {
                                        values[i] = leafField.nextValue();
                                    } catch (IOException ex) {
                                        throw new ElasticsearchException("Failed to read values for field [" + fieldName + "]");
                                    }

                                }
                                return values;
                            });
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
        private final Map<String, MetricFieldProducer> metricFieldProducers;
        private final Map<String, LabelFieldProducer> labelFieldProducers;

        RollupBucketBuilder() {
            this.metricFieldProducers = MetricFieldProducer.buildMetricFieldProducers(searchExecutionContext, metricFields);
            this.labelFieldProducers = LabelFieldProducer.buildLabelFieldProducers(searchExecutionContext, labelFields);
        }

        /**
         * tsid changed, reset tsid and timestamp
         */
        public RollupBucketBuilder resetTsid(BytesRef tsid, long timestamp) {
            this.tsid = BytesRef.deepCopyOf(tsid);
            return resetTimestamp(timestamp);
        }

        /**
         * timestamp change, reset builder
         */
        public RollupBucketBuilder resetTimestamp(long timestamp) {
            this.timestamp = timestamp;
            this.docCount = 0;
            this.metricFieldProducers.values().forEach(MetricFieldProducer::reset);
            this.labelFieldProducers.values().forEach(LabelFieldProducer::reset);
            if (logger.isTraceEnabled()) {
                logger.trace(
                    "New bucket for _tsid: [{}], @timestamp: [{}]",
                    DocValueFormat.TIME_SERIES_ID.format(tsid),
                    timestampFormat.format(timestamp)
                );
            }
            return this;
        }

        public void collect(final String field, int docValueCount, final Function<Integer, Object[]> fieldValues) {
            final Object[] values = fieldValues.apply(docValueCount);
            if (metricFieldProducers.containsKey(field)) {
                // TODO: missing support for array metrics
                collectMetric(field, values);
            } else if (labelFieldProducers.containsKey(field)) {
                if (values.length == 1) {
                    collectLabel(field, values[0]);
                } else {
                    collectLabel(field, values);
                }
            } else {
                throw new IllegalArgumentException(
                    "Field '"
                        + field
                        + "' is not a label nor a metric, existing labels: [ "
                        + String.join(",", labelFieldProducers.keySet())
                        + "], existing metrics: ["
                        + String.join(", ", metricFieldProducers.keySet())
                        + "]"
                );
            }
        }

        private void collectLabel(final String field, final Object value) {
            labelFieldProducers.get(field).collect(value);
        }

        private void collectMetric(final String field, final Object[] values) {
            for (var value : values) {
                if (value instanceof Number number) {
                    metricFieldProducers.get(field).collect(number);
                } else {
                    throw new IllegalArgumentException(
                        "Expected numeric value for field '" + field + "' but got non numeric value: '" + value + "'"
                    );
                }
            }
        }

        public void collectDocCount(int docCount) {
            this.docCount += docCount;
        }

        public Map<String, Object> buildRollupDocument() {
            if (isEmpty()) {
                return Collections.emptyMap();
            }

            // Extract dimension values from _tsid field, so we avoid loading them from doc_values
            @SuppressWarnings("unchecked")
            Map<String, Object> dimensions = (Map<String, Object>) DocValueFormat.TIME_SERIES_ID.format(tsid);
            Map<String, Object> doc = Maps.newLinkedHashMapWithExpectedSize(
                2 + dimensions.size() + metricFieldProducers.size() + labelFieldProducers.size()
            );
            doc.put(timestampField.name(), timestampFormat.format(timestamp));
            doc.put(DocCountFieldMapper.NAME, docCount);

            for (Map.Entry<String, Object> e : dimensions.entrySet()) {
                assert e.getValue() != null;
                doc.put(e.getKey(), e.getValue());
            }

            for (AbstractRollupFieldProducer<?> fieldProducer : Stream.concat(
                metricFieldProducers.values().stream(),
                labelFieldProducers.values().stream()
            ).toList()) {
                if (fieldProducer.isEmpty() == false) {
                    String field = fieldProducer.name();
                    Object value = fieldProducer.value();
                    if (value != null) {
                        doc.put(field, value);
                    }
                }
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
            return tsid() == null || timestamp() == 0 || docCount() == 0;
        }
    }
}
