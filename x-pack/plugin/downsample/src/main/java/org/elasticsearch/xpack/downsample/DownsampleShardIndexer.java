/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.downsample;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
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
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.DocCountFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
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
import org.elasticsearch.xpack.core.downsample.DownsampleAfterBulkInfo;
import org.elasticsearch.xpack.core.downsample.DownsampleBeforeBulkInfo;
import org.elasticsearch.xpack.core.downsample.DownsampleIndexerAction;
import org.elasticsearch.xpack.core.downsample.DownsampleShardIndexerStatus;
import org.elasticsearch.xpack.core.downsample.DownsampleShardPersistentTaskState;
import org.elasticsearch.xpack.core.downsample.DownsampleShardTask;

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
 * computes the downsample buckets and stores the buckets in the downsampled index.
 * <p>
 * The documents collected by the {@link TimeSeriesIndexSearcher} are expected to be sorted
 * by _tsid in ascending order and @timestamp in descending order.
 */
class DownsampleShardIndexer {

    private static final Logger logger = LogManager.getLogger(DownsampleShardIndexer.class);
    private static final int DOCID_BUFFER_SIZE = 8096;
    public static final int DOWNSAMPLE_BULK_ACTIONS = 10000;
    public static final ByteSizeValue DOWNSAMPLE_BULK_SIZE = ByteSizeValue.of(1, ByteSizeUnit.MB);
    public static final ByteSizeValue DOWNSAMPLE_MAX_BYTES_IN_FLIGHT = ByteSizeValue.of(50, ByteSizeUnit.MB);
    private final IndexShard indexShard;
    private final Client client;
    private final DownsampleMetrics downsampleMetrics;
    private final String downsampleIndex;
    private final Engine.Searcher searcher;
    private final SearchExecutionContext searchExecutionContext;
    private final DateFieldMapper.DateFieldType timestampField;
    private final DocValueFormat timestampFormat;
    private final Rounding.Prepared rounding;
    private final List<FieldValueFetcher> fieldValueFetchers;
    private final DownsampleShardTask task;
    private final DownsampleShardPersistentTaskState state;
    private final String[] dimensions;
    private volatile boolean abort = false;
    ByteSizeValue downsampleBulkSize = DOWNSAMPLE_BULK_SIZE;
    ByteSizeValue downsampleMaxBytesInFlight = DOWNSAMPLE_MAX_BYTES_IN_FLIGHT;

    DownsampleShardIndexer(
        final DownsampleShardTask task,
        final Client client,
        final IndexService indexService,
        final DownsampleMetrics downsampleMetrics,
        final ShardId shardId,
        final String downsampleIndex,
        final DownsampleConfig config,
        final String[] metrics,
        final String[] labels,
        final String[] dimensions,
        final DownsampleShardPersistentTaskState state
    ) {
        this.task = task;
        this.client = client;
        this.downsampleMetrics = downsampleMetrics;
        this.indexShard = indexService.getShard(shardId.id());
        this.downsampleIndex = downsampleIndex;
        this.searcher = indexShard.acquireSearcher("downsampling");
        this.state = state;
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
            this.dimensions = dimensions;
            this.timestampField = (DateFieldMapper.DateFieldType) searchExecutionContext.getFieldType(config.getTimestampField());
            this.timestampFormat = timestampField.docValueFormat(null, null);
            this.rounding = config.createRounding();

            List<FieldValueFetcher> fetchers = new ArrayList<>(metrics.length + labels.length + dimensions.length);
            fetchers.addAll(FieldValueFetcher.create(searchExecutionContext, metrics));
            fetchers.addAll(FieldValueFetcher.create(searchExecutionContext, labels));
            fetchers.addAll(DimensionFieldValueFetcher.create(searchExecutionContext, dimensions));
            this.fieldValueFetchers = Collections.unmodifiableList(fetchers);
            toClose = null;
        } finally {
            IOUtils.closeWhileHandlingException(toClose);
        }
    }

    public DownsampleIndexerAction.ShardDownsampleResponse execute() throws IOException {
        final Query initialStateQuery = createQuery();
        if (initialStateQuery instanceof MatchNoDocsQuery) {
            return new DownsampleIndexerAction.ShardDownsampleResponse(indexShard.shardId(), task.getNumIndexed());
        }
        long startTime = client.threadPool().relativeTimeInMillis();
        task.setTotalShardDocCount(searcher.getDirectoryReader().numDocs());
        task.setDownsampleShardIndexerStatus(DownsampleShardIndexerStatus.STARTED);
        task.updatePersistentTaskState(
            new DownsampleShardPersistentTaskState(DownsampleShardIndexerStatus.STARTED, null),
            ActionListener.noop()
        );
        logger.info("Downsampling task [" + task.getPersistentTaskId() + " on shard " + indexShard.shardId() + " started");
        BulkProcessor2 bulkProcessor = createBulkProcessor();
        try (searcher; bulkProcessor) {
            final TimeSeriesIndexSearcher timeSeriesSearcher = new TimeSeriesIndexSearcher(searcher, List.of(this::checkCancelled));
            TimeSeriesBucketCollector bucketCollector = new TimeSeriesBucketCollector(bulkProcessor, this.dimensions);
            bucketCollector.preCollection();
            timeSeriesSearcher.search(initialStateQuery, bucketCollector);
        }

        TimeValue duration = TimeValue.timeValueMillis(client.threadPool().relativeTimeInMillis() - startTime);
        logger.info(
            "Shard [{}] successfully sent [{}], received source doc [{}], indexed downsampled doc [{}], failed [{}], took [{}]",
            indexShard.shardId(),
            task.getNumReceived(),
            task.getNumSent(),
            task.getNumIndexed(),
            task.getNumFailed(),
            duration
        );

        if (task.getNumIndexed() != task.getNumSent()) {
            task.setDownsampleShardIndexerStatus(DownsampleShardIndexerStatus.FAILED);
            final String error = "Downsampling task ["
                + task.getPersistentTaskId()
                + "] on shard "
                + indexShard.shardId()
                + " failed indexing, "
                + " indexed ["
                + task.getNumIndexed()
                + "] sent ["
                + task.getNumSent()
                + "]";
            logger.info(error);
            downsampleMetrics.recordShardOperation(duration.millis(), DownsampleMetrics.ActionStatus.MISSING_DOCS);
            throw new DownsampleShardIndexerException(error, false);
        }

        if (task.getNumFailed() > 0) {
            final String error = "Downsampling task ["
                + task.getPersistentTaskId()
                + "] on shard "
                + indexShard.shardId()
                + " failed indexing ["
                + task.getNumFailed()
                + "]";
            logger.info(error);
            downsampleMetrics.recordShardOperation(duration.millis(), DownsampleMetrics.ActionStatus.FAILED);
            throw new DownsampleShardIndexerException(error, false);
        }

        task.setDownsampleShardIndexerStatus(DownsampleShardIndexerStatus.COMPLETED);
        task.updatePersistentTaskState(
            new DownsampleShardPersistentTaskState(DownsampleShardIndexerStatus.COMPLETED, null),
            ActionListener.noop()
        );
        logger.info("Downsampling task [" + task.getPersistentTaskId() + " on shard " + indexShard.shardId() + " completed");
        downsampleMetrics.recordShardOperation(duration.millis(), DownsampleMetrics.ActionStatus.SUCCESS);
        return new DownsampleIndexerAction.ShardDownsampleResponse(indexShard.shardId(), task.getNumIndexed());
    }

    private Query createQuery() {
        if (this.state.started() && this.state.tsid() != null) {
            return SortedSetDocValuesField.newSlowRangeQuery(TimeSeriesIdFieldMapper.NAME, this.state.tsid(), null, true, false);
        }
        return new MatchAllDocsQuery();
    }

    private void checkCancelled() {
        if (task.isCancelled()) {
            logger.warn(
                "Shard [{}] downsampled abort, sent [{}], indexed [{}], failed[{}]",
                indexShard.shardId(),
                task.getNumSent(),
                task.getNumIndexed(),
                task.getNumFailed()
            );
            task.setDownsampleShardIndexerStatus(DownsampleShardIndexerStatus.CANCELLED);
            task.updatePersistentTaskState(
                new DownsampleShardPersistentTaskState(DownsampleShardIndexerStatus.CANCELLED, null),
                ActionListener.noop()
            );
            logger.info("Downsampling task [" + task.getPersistentTaskId() + "] on shard " + indexShard.shardId() + " cancelled");
            throw new DownsampleShardIndexerException(
                new TaskCancelledException(format("Shard %s downsample cancelled", indexShard.shardId())),
                format("Shard %s downsample cancelled", indexShard.shardId()),
                false
            );

        }
        if (abort) {
            logger.warn(
                "Shard [{}] downsample abort, sent [{}], indexed [{}], failed[{}]",
                indexShard.shardId(),
                task.getNumSent(),
                task.getNumIndexed(),
                task.getNumFailed()
            );
            task.setDownsampleShardIndexerStatus(DownsampleShardIndexerStatus.FAILED);
            task.updatePersistentTaskState(
                new DownsampleShardPersistentTaskState(DownsampleShardIndexerStatus.FAILED, null),
                ActionListener.noop()
            );
            throw new DownsampleShardIndexerException("Bulk indexing failure", true);
        }
    }

    private BulkProcessor2 createBulkProcessor() {
        final BulkProcessor2.Listener listener = new BulkProcessor2.Listener() {

            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                task.addNumSent(request.numberOfActions());
                task.setBeforeBulkInfo(
                    new DownsampleBeforeBulkInfo(
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
                    new DownsampleAfterBulkInfo(
                        client.threadPool().absoluteTimeInMillis(),
                        executionId,
                        bulkIngestTookMillis,
                        bulkTookMillis,
                        response.hasFailures(),
                        RestStatus.OK.getStatus()
                    )
                );
                task.updateBulkInfo(bulkIngestTookMillis, bulkTookMillis);

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
                    logger.error("Shard [{}] failed to populate downsample index. Failures: [{}]", indexShard.shardId(), failures);

                    abort = true;
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Exception failure) {
                if (failure != null) {
                    long items = request.numberOfActions();
                    task.addNumFailed(items);
                    logger.error(() -> format("Shard [%s] failed to populate downsample index.", indexShard.shardId()), failure);

                    abort = true;
                }
            }
        };

        return BulkProcessor2.builder(client::bulk, listener, client.threadPool())
            .setBulkActions(DOWNSAMPLE_BULK_ACTIONS)
            .setBulkSize(DOWNSAMPLE_BULK_SIZE)
            .setMaxBytesInFlight(downsampleMaxBytesInFlight)
            .setMaxNumberOfRetries(3)
            .build();
    }

    private class TimeSeriesBucketCollector extends BucketCollector {
        private final BulkProcessor2 bulkProcessor;
        private final DownsampleBucketBuilder downsampleBucketBuilder;
        private final List<LeafDownsampleCollector> leafBucketCollectors = new ArrayList<>();
        private long docsProcessed;
        private long bucketsCreated;
        long lastTimestamp = Long.MAX_VALUE;
        long lastHistoTimestamp = Long.MAX_VALUE;

        TimeSeriesBucketCollector(BulkProcessor2 bulkProcessor, String[] dimensions) {
            this.bulkProcessor = bulkProcessor;
            AbstractDownsampleFieldProducer[] fieldProducers = fieldValueFetchers.stream()
                .map(FieldValueFetcher::fieldProducer)
                .toArray(AbstractDownsampleFieldProducer[]::new);
            this.downsampleBucketBuilder = new DownsampleBucketBuilder(fieldProducers, dimensions);
        }

        @Override
        public LeafBucketCollector getLeafCollector(final AggregationExecutionContext aggCtx) throws IOException {
            final LeafReaderContext ctx = aggCtx.getLeafReaderContext();
            final DocCountProvider docCountProvider = new DocCountProvider();
            docCountProvider.setLeafReaderContext(ctx);

            // For each field, return a tuple with the downsample field producer and the field value leaf
            final List<AbstractDownsampleFieldProducer> nonMetricProducers = new ArrayList<>();
            final List<FormattedDocValues> formattedDocValues = new ArrayList<>();

            final List<MetricFieldProducer> metricProducers = new ArrayList<>();
            final List<SortedNumericDoubleValues> numericDocValues = new ArrayList<>();
            for (var fieldValueFetcher : fieldValueFetchers) {
                var fieldProducer = fieldValueFetcher.fieldProducer();
                if (fieldProducer instanceof MetricFieldProducer metricFieldProducer) {
                    metricProducers.add(metricFieldProducer);
                    numericDocValues.add(fieldValueFetcher.getNumericLeaf(ctx));
                } else {
                    nonMetricProducers.add(fieldProducer);
                    formattedDocValues.add(fieldValueFetcher.getLeaf(ctx));
                }
            }

            var leafBucketCollector = new LeafDownsampleCollector(
                aggCtx,
                docCountProvider,
                nonMetricProducers.toArray(new AbstractDownsampleFieldProducer[0]),
                formattedDocValues.toArray(new FormattedDocValues[0]),
                metricProducers.toArray(new MetricFieldProducer[0]),
                numericDocValues.toArray(new SortedNumericDoubleValues[0])
            );
            leafBucketCollectors.add(leafBucketCollector);
            return leafBucketCollector;
        }

        void bulkCollection() throws IOException {
            // The leaf bucket collectors with newer timestamp go first, to correctly capture the last value for counters and labels.
            leafBucketCollectors.sort((o1, o2) -> -Long.compare(o1.firstTimeStampForBulkCollection, o2.firstTimeStampForBulkCollection));
            for (LeafDownsampleCollector leafBucketCollector : leafBucketCollectors) {
                leafBucketCollector.leafBulkCollection();
            }
        }

        class LeafDownsampleCollector extends LeafBucketCollector {

            final AggregationExecutionContext aggCtx;
            final DocCountProvider docCountProvider;
            final FormattedDocValues[] formattedDocValues;
            final AbstractDownsampleFieldProducer[] nonMetricProducers;

            final MetricFieldProducer[] metricProducers;
            final SortedNumericDoubleValues[] numericDocValues;

            // Capture the first timestamp in order to determine which leaf collector's leafBulkCollection() is invoked first.
            long firstTimeStampForBulkCollection;
            final IntArrayList docIdBuffer = new IntArrayList(DOCID_BUFFER_SIZE);
            final long timestampBoundStartTime = searchExecutionContext.getIndexSettings().getTimestampBounds().startTime();

            LeafDownsampleCollector(
                AggregationExecutionContext aggCtx,
                DocCountProvider docCountProvider,
                AbstractDownsampleFieldProducer[] nonMetricProducers,
                FormattedDocValues[] formattedDocValues,
                MetricFieldProducer[] metricProducers,
                SortedNumericDoubleValues[] numericDocValues
            ) {
                assert nonMetricProducers.length == formattedDocValues.length;
                assert metricProducers.length == numericDocValues.length;

                this.aggCtx = aggCtx;
                this.docCountProvider = docCountProvider;
                this.nonMetricProducers = nonMetricProducers;
                this.formattedDocValues = formattedDocValues;
                this.metricProducers = metricProducers;
                this.numericDocValues = numericDocValues;
            }

            @Override
            public void collect(int docId, long owningBucketOrd) throws IOException {
                task.addNumReceived(1);
                final BytesRef tsidHash = aggCtx.getTsidHash();
                assert tsidHash != null : "Document without [" + TimeSeriesIdFieldMapper.NAME + "] field was found.";
                final int tsidHashOrd = aggCtx.getTsidHashOrd();
                final long timestamp = timestampField.resolution().roundDownToMillis(aggCtx.getTimestamp());

                boolean tsidChanged = tsidHashOrd != downsampleBucketBuilder.tsidOrd();
                if (tsidChanged || timestamp < lastHistoTimestamp) {
                    lastHistoTimestamp = Math.max(rounding.round(timestamp), timestampBoundStartTime);
                }
                task.setLastSourceTimestamp(timestamp);
                task.setLastTargetTimestamp(lastHistoTimestamp);

                if (logger.isTraceEnabled()) {
                    logger.trace(
                        "Doc: [{}] - _tsid: [{}], @timestamp: [{}] -> downsample bucket ts: [{}]",
                        docId,
                        DocValueFormat.TIME_SERIES_ID.format(tsidHash),
                        timestampFormat.format(timestamp),
                        timestampFormat.format(lastHistoTimestamp)
                    );
                }

                assert assertTsidAndTimestamp(tsidHash, timestamp);
                lastTimestamp = timestamp;

                if (tsidChanged || downsampleBucketBuilder.timestamp() != lastHistoTimestamp) {
                    bulkCollection();
                    // Flush downsample doc if not empty
                    if (downsampleBucketBuilder.isEmpty() == false) {
                        XContentBuilder doc = downsampleBucketBuilder.buildDownsampleDocument();
                        indexBucket(doc);
                    }

                    // Create new downsample bucket
                    if (tsidChanged) {
                        downsampleBucketBuilder.resetTsid(tsidHash, tsidHashOrd, lastHistoTimestamp);
                    } else {
                        downsampleBucketBuilder.resetTimestamp(lastHistoTimestamp);
                    }
                    bucketsCreated++;
                }

                if (docIdBuffer.isEmpty()) {
                    firstTimeStampForBulkCollection = aggCtx.getTimestamp();
                }
                // buffer.add() always delegates to system.arraycopy() and checks buffer size for resizing purposes:
                docIdBuffer.buffer[docIdBuffer.elementsCount++] = docId;
                if (docIdBuffer.size() == DOCID_BUFFER_SIZE) {
                    bulkCollection();
                }
            }

            void leafBulkCollection() throws IOException {
                if (docIdBuffer.isEmpty()) {
                    return;
                }

                if (logger.isDebugEnabled()) {
                    logger.debug("buffered {} docids", docIdBuffer.size());
                }

                downsampleBucketBuilder.collectDocCount(docIdBuffer, docCountProvider);
                // Iterate over all field values and collect the doc_values for this docId
                for (int i = 0; i < nonMetricProducers.length; i++) {
                    AbstractDownsampleFieldProducer fieldProducer = nonMetricProducers[i];
                    FormattedDocValues docValues = formattedDocValues[i];
                    fieldProducer.collect(docValues, docIdBuffer);
                }
                for (int i = 0; i < metricProducers.length; i++) {
                    MetricFieldProducer metricFieldProducer = metricProducers[i];
                    SortedNumericDoubleValues numericDoubleValues = numericDocValues[i];
                    metricFieldProducer.collect(numericDoubleValues, docIdBuffer);
                }

                docsProcessed += docIdBuffer.size();
                task.setDocsProcessed(docsProcessed);

                // buffer.clean() also overwrites all slots with zeros
                docIdBuffer.elementsCount = 0;
            }

            /**
             * Sanity checks to ensure that we receive documents in the correct order
             * - _tsid must be sorted in ascending order
             * - @timestamp must be sorted in descending order within the same _tsid
             */
            boolean assertTsidAndTimestamp(BytesRef tsidHash, long timestamp) {
                BytesRef lastTsid = downsampleBucketBuilder.tsid();
                assert lastTsid == null || lastTsid.compareTo(tsidHash) <= 0
                    : "_tsid is not sorted in ascending order: ["
                        + DocValueFormat.TIME_SERIES_ID.format(lastTsid)
                        + "] -> ["
                        + DocValueFormat.TIME_SERIES_ID.format(tsidHash)
                        + "]";
                assert tsidHash.equals(lastTsid) == false || lastTimestamp >= timestamp
                    : "@timestamp is not sorted in descending order: ["
                        + timestampFormat.format(lastTimestamp)
                        + "] -> ["
                        + timestampFormat.format(timestamp)
                        + "]";
                return true;
            }
        }

        private void indexBucket(XContentBuilder doc) {
            IndexRequestBuilder request = client.prepareIndex(downsampleIndex);
            request.setSource(doc);
            if (logger.isTraceEnabled()) {
                logger.trace("Indexing downsample doc: [{}]", Strings.toString(doc));
            }
            IndexRequest indexRequest = request.request();
            task.setLastIndexingTimestamp(System.currentTimeMillis());
            bulkProcessor.addWithBackpressure(indexRequest, () -> abort);
        }

        @Override
        public void preCollection() {
            // check cancel when start running
            checkCancelled();
        }

        @Override
        public void postCollection() throws IOException {
            // Flush downsample doc if not empty
            bulkCollection();
            if (downsampleBucketBuilder.isEmpty() == false) {
                XContentBuilder doc = downsampleBucketBuilder.buildDownsampleDocument();
                indexBucket(doc);
            }

            // check cancel after the flush all data
            checkCancelled();

            logger.info(
                "Shard {} processed [{}] docs, created [{}] downsample buckets",
                indexShard.shardId(),
                docsProcessed,
                bucketsCreated
            );
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
        }
    }

    private class DownsampleBucketBuilder {
        private BytesRef tsid;
        private int tsidOrd = -1;
        private long timestamp;
        private int docCount;
        private final AbstractDownsampleFieldProducer[] fieldProducers;
        private final DownsampleFieldSerializer[] groupedProducers;
        private final String[] dimensions;

        DownsampleBucketBuilder(AbstractDownsampleFieldProducer[] fieldProducers, String[] dimensions) {
            this.fieldProducers = fieldProducers;
            this.dimensions = dimensions;
            /*
             * The downsample field producers for aggregate_metric_double all share the same name (this is
             * the name they will be serialized in the target index). We group all field producers by
             * name. If grouping yields multiple downsample field producers, we delegate serialization to
             * the AggregateMetricFieldSerializer class.
             */
            groupedProducers = Arrays.stream(fieldProducers)
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
                .toArray(DownsampleFieldSerializer[]::new);
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
            for (AbstractDownsampleFieldProducer producer : fieldProducers) {
                producer.reset();
            }
            if (logger.isTraceEnabled()) {
                logger.trace(
                    "New bucket for _tsid: [{}], @timestamp: [{}]",
                    DocValueFormat.TIME_SERIES_ID.format(tsid),
                    timestampFormat.format(timestamp)
                );
            }
        }

        public void collectDocCount(IntArrayList buffer, DocCountProvider docCountProvider) throws IOException {
            if (docCountProvider.alwaysOne()) {
                this.docCount += buffer.size();
            } else {
                for (int i = 0; i < buffer.size(); i++) {
                    int docId = buffer.get(i);
                    this.docCount += docCountProvider.getDocCount(docId);
                }
            }
        }

        public XContentBuilder buildDownsampleDocument() throws IOException {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.SMILE);
            builder.startObject();
            if (isEmpty()) {
                builder.endObject();
                return builder;
            }
            builder.field(timestampField.name(), timestampFormat.format(timestamp));
            builder.field(DocCountFieldMapper.NAME, docCount);

            // Serialize fields
            for (DownsampleFieldSerializer fieldProducer : groupedProducers) {
                fieldProducer.write(builder);
            }

            if (dimensions.length == 0) {
                logger.debug("extracting dimensions from legacy tsid");
                Map<?, ?> dimensions = (Map<?, ?>) DocValueFormat.TIME_SERIES_ID.format(tsid);
                for (Map.Entry<?, ?> e : dimensions.entrySet()) {
                    assert e.getValue() != null;
                    builder.field((String) e.getKey(), e.getValue());
                }
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
