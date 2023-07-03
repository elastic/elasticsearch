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
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor2;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.DocCountFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.DocCountProvider;
import org.elasticsearch.search.aggregations.support.TimeSeriesIndexSearcher;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.downsample.DownsampleIndexerAction;
import org.elasticsearch.xpack.core.rollup.action.RollupAfterBulkInfo;
import org.elasticsearch.xpack.core.rollup.action.RollupBeforeBulkInfo;
import org.elasticsearch.xpack.core.rollup.action.RollupShardIndexerStatus;
import org.elasticsearch.xpack.core.rollup.action.RollupShardTask;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;
import static org.elasticsearch.core.Strings.format;

/**
 * An indexer for downsampling that iterates documents collected by {@link TimeSeriesIndexSearcher},
 * computes the rollup buckets and stores the buckets in the downsampled index.
 * <p>
 * The documents collected by the {@link TimeSeriesIndexSearcher} are expected to be sorted
 * by _tsid in ascending order and @timestamp in descending order.
 */
class RollupShardIndexer {

    private static final Logger logger = LogManager.getLogger(RollupShardIndexer.class);
    public static final int ROLLUP_BULK_ACTIONS = 10000;
    public static final ByteSizeValue ROLLUP_BULK_SIZE = new ByteSizeValue(1, ByteSizeUnit.MB);
    public static final ByteSizeValue ROLLUP_MAX_BYTES_IN_FLIGHT = new ByteSizeValue(50, ByteSizeUnit.MB);

    private final IndexShard indexShard;
    private final Client client;
    private final String rollupIndex;
    private final Engine.Searcher searcher;
    private final SearchExecutionContext searchExecutionContext;
    private final DateFieldMapper.DateFieldType timestampField;
    private final DocValueFormat timestampFormat;
    private final Rounding.Prepared rounding;
    private final List<FieldValueFetcher> fieldValueFetchers;
    private final RollupShardTask task;
    private volatile boolean abort = false;
    ByteSizeValue rollupBulkSize = ROLLUP_BULK_SIZE;
    ByteSizeValue rollupMaxBytesInFlight = ROLLUP_MAX_BYTES_IN_FLIGHT;

    RollupShardIndexer(
        RollupShardTask task,
        Client client,
        IndexService indexService,
        ShardId shardId,
        String rollupIndex,
        DownsampleConfig config,
        String[] metricFields,
        String[] labelFields
    ) {
        this.task = task;
        this.client = client;
        this.indexShard = indexService.getShard(shardId.id());
        this.rollupIndex = rollupIndex;
        this.searcher = indexShard.acquireSearcher("downsampling");
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
            this.timestampField = (DateFieldMapper.DateFieldType) searchExecutionContext.getFieldType(config.getTimestampField());
            this.timestampFormat = timestampField.docValueFormat(null, null);
            this.rounding = config.createRounding();

            List<FieldValueFetcher> fetchers = new ArrayList<>(metricFields.length + labelFields.length);
            fetchers.addAll(FieldValueFetcher.create(searchExecutionContext, metricFields));
            fetchers.addAll(FieldValueFetcher.create(searchExecutionContext, labelFields));
            this.fieldValueFetchers = Collections.unmodifiableList(fetchers);
            toClose = null;
        } finally {
            IOUtils.closeWhileHandlingException(toClose);
        }
    }

    public DownsampleIndexerAction.ShardDownsampleResponse execute() throws IOException {
        long startTime = client.threadPool().relativeTimeInMillis();
        task.setTotalShardDocCount(searcher.getDirectoryReader().numDocs());
        task.setRollupShardIndexerStatus(RollupShardIndexerStatus.STARTED);
        BulkProcessor2 bulkProcessor = createBulkProcessor();
        try (searcher; bulkProcessor) {
            final TimeSeriesIndexSearcher timeSeriesSearcher = new TimeSeriesIndexSearcher(searcher, List.of(this::checkCancelled));
            TimeSeriesBucketCollector bucketCollector = new TimeSeriesBucketCollector(bulkProcessor);
            bucketCollector.preCollection();
            timeSeriesSearcher.search(new MatchAllDocsQuery(), bucketCollector);
            bucketCollector.postCollection();
        }

        logger.info(
            "Shard [{}] successfully sent [{}], received source doc [{}], indexed rollup doc [{}], failed [{}], took [{}]",
            indexShard.shardId(),
            task.getNumReceived(),
            task.getNumSent(),
            task.getNumIndexed(),
            task.getNumFailed(),
            TimeValue.timeValueMillis(client.threadPool().relativeTimeInMillis() - startTime)
        );

        if (task.getNumIndexed() != task.getNumSent()) {
            task.setRollupShardIndexerStatus(RollupShardIndexerStatus.FAILED);
            throw new ElasticsearchException(
                "Shard ["
                    + indexShard.shardId()
                    + "] failed to index all rollup documents. Sent ["
                    + task.getNumSent()
                    + "], indexed ["
                    + task.getNumIndexed()
                    + "]."
            );
        }

        if (task.getNumFailed() > 0) {
            task.setRollupShardIndexerStatus(RollupShardIndexerStatus.FAILED);
            throw new ElasticsearchException(
                "Shard ["
                    + indexShard.shardId()
                    + "] failed to index all rollup documents. Sent ["
                    + task.getNumSent()
                    + "], failed ["
                    + task.getNumFailed()
                    + "]."
            );
        }

        task.setRollupShardIndexerStatus(RollupShardIndexerStatus.COMPLETED);

        return new DownsampleIndexerAction.ShardDownsampleResponse(indexShard.shardId(), task.getNumIndexed());
    }

    private void checkCancelled() {
        if (task.isCancelled() || abort) {
            logger.warn(
                "Shard [{}] rollup abort, sent [{}], indexed [{}], failed[{}]",
                indexShard.shardId(),
                task.getNumSent(),
                task.getNumIndexed(),
                task.getNumFailed()
            );
            task.setRollupShardIndexerStatus(RollupShardIndexerStatus.CANCELLED);
            throw new TaskCancelledException(format("Shard %s rollup cancelled", indexShard.shardId()));
        }
    }

    private BulkProcessor2 createBulkProcessor() {
        final BulkProcessor2.Listener listener = new BulkProcessor2.Listener() {

            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                task.addNumSent(request.numberOfActions());
                task.setBeforeBulkInfo(
                    new RollupBeforeBulkInfo(
                        client.threadPool().absoluteTimeInMillis(),
                        executionId,
                        request.estimatedSizeInBytes(),
                        request.numberOfActions()
                    )
                );
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                long bulkIngestTookMillis = response.getIngestTookInMillis() >= 0 ? response.getIngestTookInMillis() : 0;
                long bulkTookMillis = response.getTook().getMillis();
                task.addNumIndexed(request.numberOfActions());
                task.setAfterBulkInfo(
                    new RollupAfterBulkInfo(
                        client.threadPool().absoluteTimeInMillis(),
                        executionId,
                        bulkIngestTookMillis,
                        bulkTookMillis,
                        response.hasFailures(),
                        response.status().getStatus()
                    )
                );
                task.updateRollupBulkInfo(bulkIngestTookMillis, bulkTookMillis);

                if (response.hasFailures()) {
                    List<BulkItemResponse> failedItems = Arrays.stream(response.getItems()).filter(BulkItemResponse::isFailed).toList();
                    task.addNumFailed(failedItems.size());

                    Map<String, String> failures = failedItems.stream()
                        .collect(
                            Collectors.toMap(
                                BulkItemResponse::getId,
                                BulkItemResponse::getFailureMessage,
                                (msg1, msg2) -> Objects.equals(msg1, msg2) ? msg1 : msg1 + "," + msg2
                            )
                        );
                    logger.error("Shard [{}] failed to populate rollup index. Failures: [{}]", indexShard.shardId(), failures);

                    // cancel rollup task
                    abort = true;
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Exception failure) {
                if (failure != null) {
                    long items = request.numberOfActions();
                    task.addNumFailed(items);
                    logger.error(() -> format("Shard [%s] failed to populate rollup index.", indexShard.shardId()), failure);

                    // cancel rollup task
                    abort = true;
                }
            }
        };

        return BulkProcessor2.builder(client::bulk, listener, client.threadPool())
            .setBulkActions(ROLLUP_BULK_ACTIONS)
            .setBulkSize(ROLLUP_BULK_SIZE)
            .setMaxBytesInFlight(rollupMaxBytesInFlight)
            .setMaxNumberOfRetries(3)
            .build();
    }

    private class TimeSeriesBucketCollector extends BucketCollector {
        private final BulkProcessor2 bulkProcessor;
        private final RollupBucketBuilder rollupBucketBuilder;
        private long docsProcessed;
        private long bucketsCreated;
        long lastTimestamp = Long.MAX_VALUE;
        long lastHistoTimestamp = Long.MAX_VALUE;

        TimeSeriesBucketCollector(BulkProcessor2 bulkProcessor) {
            this.bulkProcessor = bulkProcessor;
            List<AbstractDownsampleFieldProducer> rollupFieldProducers = fieldValueFetchers.stream()
                .map(FieldValueFetcher::rollupFieldProducer)
                .toList();
            this.rollupBucketBuilder = new RollupBucketBuilder(rollupFieldProducers);
        }

        @Override
        public LeafBucketCollector getLeafCollector(final AggregationExecutionContext aggCtx) throws IOException {
            final LeafReaderContext ctx = aggCtx.getLeafReaderContext();
            final DocCountProvider docCountProvider = new DocCountProvider();
            docCountProvider.setLeafReaderContext(ctx);

            // For each field, return a tuple with the rollup field producer and the field value leaf
            final List<Tuple<AbstractDownsampleFieldProducer, FormattedDocValues>> fieldValueTuples = fieldValueFetchers.stream()
                .map(fetcher -> Tuple.tuple(fetcher.rollupFieldProducer(), fetcher.getLeaf(ctx)))
                .toList();

            return new LeafBucketCollector() {
                @Override
                public void collect(int docId, long owningBucketOrd) throws IOException {
                    task.addNumReceived(1);
                    final BytesRef tsid = aggCtx.getTsid();
                    assert tsid != null : "Document without [" + TimeSeriesIdFieldMapper.NAME + "] field was found.";
                    final int tsidOrd = aggCtx.getTsidOrd();
                    final long timestamp = timestampField.resolution().roundDownToMillis(aggCtx.getTimestamp());

                    boolean tsidChanged = tsidOrd != rollupBucketBuilder.tsidOrd();
                    if (tsidChanged || timestamp < lastHistoTimestamp) {
                        lastHistoTimestamp = Math.max(
                            rounding.round(timestamp),
                            searchExecutionContext.getIndexSettings().getTimestampBounds().startTime()
                        );
                    }
                    task.setLastSourceTimestamp(timestamp);
                    task.setLastTargetTimestamp(lastHistoTimestamp);

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
                            XContentBuilder doc = rollupBucketBuilder.buildRollupDocument();
                            indexBucket(doc);
                        }

                        // Create new rollup bucket
                        if (tsidChanged) {
                            rollupBucketBuilder.resetTsid(tsid, tsidOrd, lastHistoTimestamp);
                        } else {
                            rollupBucketBuilder.resetTimestamp(lastHistoTimestamp);
                        }
                        bucketsCreated++;
                    }

                    final int docCount = docCountProvider.getDocCount(docId);
                    rollupBucketBuilder.collectDocCount(docCount);
                    // Iterate over all field values and collect the doc_values for this docId
                    for (Tuple<AbstractDownsampleFieldProducer, FormattedDocValues> tuple : fieldValueTuples) {
                        AbstractDownsampleFieldProducer rollupFieldProducer = tuple.v1();
                        FormattedDocValues docValues = tuple.v2();
                        rollupFieldProducer.collect(docValues, docId);
                    }
                    docsProcessed++;
                    task.setDocsProcessed(docsProcessed);
                }
            };
        }

        private void indexBucket(XContentBuilder doc) {
            IndexRequestBuilder request = client.prepareIndex(rollupIndex);
            request.setSource(doc);
            if (logger.isTraceEnabled()) {
                logger.trace("Indexing rollup doc: [{}]", Strings.toString(doc));
            }
            IndexRequest indexRequest = request.request();
            task.setLastIndexingTimestamp(System.currentTimeMillis());
            bulkProcessor.addWithBackpressure(indexRequest, () -> abort);
        }

        @Override
        public void preCollection() throws IOException {
            // check cancel when start running
            checkCancelled();
        }

        @Override
        public void postCollection() throws IOException {
            // Flush rollup doc if not empty
            if (rollupBucketBuilder.isEmpty() == false) {
                XContentBuilder doc = rollupBucketBuilder.buildRollupDocument();
                indexBucket(doc);
            }

            // check cancel after the flush all data
            checkCancelled();

            logger.info("Shard {} processed [{}] docs, created [{}] rollup buckets", indexShard.shardId(), docsProcessed, bucketsCreated);
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
        }
    }

    private class RollupBucketBuilder {
        private BytesRef tsid;
        private int tsidOrd = -1;
        private long timestamp;
        private int docCount;
        private final List<AbstractDownsampleFieldProducer> rollupFieldProducers;
        private final List<DownsampleFieldSerializer> groupedProducers;

        RollupBucketBuilder(List<AbstractDownsampleFieldProducer> rollupFieldProducers) {
            this.rollupFieldProducers = rollupFieldProducers;
            /*
             * The rollup field producers for aggregate_metric_double all share the same name (this is
             * the name they will be serialized in the target index). We group all field producers by
             * name. If grouping yields multiple rollup field producers, we delegate serialization to
             * the AggregateMetricFieldSerializer class.
             */
            groupedProducers = rollupFieldProducers.stream()
                .collect(groupingBy(AbstractDownsampleFieldProducer::name))
                .entrySet()
                .stream()
                .map(e -> {
                    if (e.getValue().size() == 1) {
                        return e.getValue().get(0);
                    } else {
                        return new AggregateMetricFieldSerializer(e.getKey(), e.getValue());
                    }
                })
                .toList();
        }

        /**
         * tsid changed, reset tsid and timestamp
         */
        public void resetTsid(BytesRef tsid, int tsidOrd, long timestamp) {
            this.tsid = BytesRef.deepCopyOf(tsid);
            this.tsidOrd = tsidOrd;
            resetTimestamp(timestamp);
        }

        /**
         * timestamp change, reset builder
         */
        public void resetTimestamp(long timestamp) {
            this.timestamp = timestamp;
            this.docCount = 0;
            this.rollupFieldProducers.forEach(AbstractDownsampleFieldProducer::reset);
            if (logger.isTraceEnabled()) {
                logger.trace(
                    "New bucket for _tsid: [{}], @timestamp: [{}]",
                    DocValueFormat.TIME_SERIES_ID.format(tsid),
                    timestampFormat.format(timestamp)
                );
            }
        }

        public void collectDocCount(int docCount) {
            this.docCount += docCount;
        }

        public XContentBuilder buildRollupDocument() throws IOException {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.SMILE);
            builder.startObject();
            if (isEmpty()) {
                builder.endObject();
                return builder;
            }

            builder.field(timestampField.name(), timestampFormat.format(timestamp));
            builder.field(DocCountFieldMapper.NAME, docCount);
            // Extract dimension values from _tsid field, so we avoid loading them from doc_values
            Map<?, ?> dimensions = (Map<?, ?>) DocValueFormat.TIME_SERIES_ID.format(tsid);
            for (Map.Entry<?, ?> e : dimensions.entrySet()) {
                assert e.getValue() != null;
                builder.field((String) e.getKey(), e.getValue());
            }

            // Serialize fields
            for (DownsampleFieldSerializer fieldProducer : groupedProducers) {
                fieldProducer.write(builder);
            }

            builder.endObject();
            return builder;
        }

        public long timestamp() {
            return timestamp;
        }

        public BytesRef tsid() {
            return tsid;
        }

        public int tsidOrd() {
            return tsidOrd;
        }

        public int docCount() {
            return docCount;
        }

        public boolean isEmpty() {
            return tsid() == null || timestamp() == 0 || docCount() == 0;
        }

    }
}
