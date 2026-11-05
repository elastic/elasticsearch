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
import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.internal.hppc.LongArrayList;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor2;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.fielddata.HistogramValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericLongValues;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.DocCountFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.lucene.queries.XSortedSetDocValuesRangeQuery;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
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
import org.elasticsearch.xpack.core.exponentialhistogram.fielddata.ExponentialHistogramValuesReader;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
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
    /**
     * How long a failing task waits for the resume watermark to be published before it lets itself be reassigned. The
     * publication must land before the reassignment, which invalidates the task's allocation, or it is rejected and the
     * reassigned task rescans the whole shard.
     */
    static final TimeValue WATERMARK_PUBLISH_TIMEOUT = TimeValue.timeValueSeconds(30);
    /**
     * How often, at most, a running task publishes the resume watermark in its state, so that a task reassigned after its node
     * is lost resumes from it. Every publication is a cluster state update on the master, and many shard tasks may run at once,
     * hence the throttling; losing an interval's worth of work on a node loss is acceptable. A node setting rather than a
     * constant so that tests can observe the publication; it is not expected to need tuning.
     */
    public static final Setting<TimeValue> WATERMARK_PUBLISH_INTERVAL_SETTING = Setting.timeSetting(
        "downsample.resume_watermark.publish_interval",
        TimeValue.timeValueMinutes(10),
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );
    private final IndexShard indexShard;
    private final Client client;
    private final DownsampleMetrics downsampleMetrics;
    private final String downsampleIndex;
    private final Engine.Searcher searcher;
    private final SearchExecutionContext searchExecutionContext;
    private final DateFieldMapper.DateFieldType timestampField;
    private final DocValueFormat timestampFormat;
    private final Rounding.Prepared rounding;
    private final List<AbstractFieldDownsampler<?>> fieldDownsamplers;
    private final TimestampValueFetcher timestampValueFetcher;
    private final DownsampleShardTask task;
    private final DownsampleShardPersistentTaskState state;
    private final String[] dimensions;
    private final AbstractFieldDownsampler.DownsamplerCountPerValueType fieldCounts;
    private final String temporalityFieldName;
    private volatile boolean abort = false;
    private final ResumeWatermark resumeWatermark;
    /**
     * The thread scanning the shard. It is the only thread adding to the bulk processor, so it is also the thread that cuts
     * the bulks and registers them with the watermark in the order they are sent, which the watermark relies on.
     */
    private Thread scanThread;
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
        final Map<String, String> multiFieldSources,
        final DownsampleShardPersistentTaskState state
    ) {
        this.task = task;
        this.client = client;
        this.downsampleMetrics = downsampleMetrics;
        this.indexShard = indexService.getShard(shardId.id());
        this.downsampleIndex = downsampleIndex;
        this.searcher = indexShard.acquireSearcher("downsampling");
        this.state = state;
        this.resumeWatermark = new ResumeWatermark(
            state.tsid(),
            WATERMARK_PUBLISH_INTERVAL_SETTING.get(client.settings()).millis(),
            client.threadPool().relativeTimeInMillis()
        );
        Closeable toClose = searcher;
        try {
            this.searchExecutionContext = indexService.newSearchExecutionContext(
                indexShard.shardId().id(),
                0,
                searcher,
                () -> 0L,
                null,
                Collections.emptyMap(),
                null,
                null
            );
            this.dimensions = dimensions;
            this.temporalityFieldName = IndexSettings.TIME_SERIES_TEMPORALITY_FIELD.get(
                searchExecutionContext.getIndexSettings().getSettings()
            );
            this.timestampField = (DateFieldMapper.DateFieldType) searchExecutionContext.getFieldType(config.getTimestampField());
            this.timestampFormat = timestampField.docValueFormat(null, null);
            this.rounding = config.createRounding();
            this.fieldCounts = new AbstractFieldDownsampler.DownsamplerCountPerValueType();
            var samplingMethod = config.getSamplingMethodOrDefault();

            List<AbstractFieldDownsampler<?>> downsamplers = new ArrayList<>(metrics.length + labels.length + dimensions.length);
            downsamplers.addAll(
                AbstractFieldDownsampler.create(searchExecutionContext, metrics, multiFieldSources, samplingMethod, fieldCounts)
            );
            // Labels are downsampled using the last value, they are not influenced by the requested sampling method
            downsamplers.addAll(
                AbstractFieldDownsampler.create(
                    searchExecutionContext,
                    labels,
                    multiFieldSources,
                    DownsampleConfig.SamplingMethod.LAST_VALUE,
                    fieldCounts
                )
            );
            downsamplers.addAll(DimensionFieldDownsampler.create(searchExecutionContext, dimensions, multiFieldSources, fieldCounts));
            this.timestampValueFetcher = new TimestampValueFetcher(timestampField, searchExecutionContext);
            this.fieldDownsamplers = Collections.unmodifiableList(downsamplers);
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
        // The resume point is republished so that a task reassigned before the watermark moves resumes from it again
        task.updatePersistentTaskState(
            new DownsampleShardPersistentTaskState(DownsampleShardIndexerStatus.STARTED, state.tsid(), state.targetIndexUuid()),
            ActionListener.noop()
        );
        logger.info(
            "Downsampling task [{}] on shard {} started{}",
            task.getPersistentTaskId(),
            indexShard.shardId(),
            state.tsid() == null ? "" : ", resuming from the watermark of a previous run"
        );
        scanThread = Thread.currentThread();
        BulkProcessor2 bulkProcessor = createBulkProcessor();
        try (searcher; bulkProcessor) {
            final TimeSeriesIndexSearcher timeSeriesSearcher = new TimeSeriesIndexSearcher(searcher, List.of(this::checkCancelled));
            TimeSeriesBucketCollector bucketCollector = new TimeSeriesBucketCollector(bulkProcessor, this.dimensions);
            bucketCollector.preCollection();
            try {
                timeSeriesSearcher.search(initialStateQuery, bucketCollector);
            } catch (Exception e) {
                // A bulk failure stops the scan in one of two ways: checkCancelled throws when it sees the abort flag, or the bulk
                // processor rejects the next bucket. Either way it is handled below, once the processor is closed and every bulk
                // has settled, so that the published watermark is exact. Any other failure is rethrown.
                if (abort == false) {
                    throw e;
                }
                logger.debug(() -> format("Shard [%s] downsample scan stopped after a bulk failure", indexShard.shardId()), e);
            }
        }

        TimeValue duration = TimeValue.timeValueMillis(client.threadPool().relativeTimeInMillis() - startTime);
        logger.info(
            "Shard [{}] successfully sent [{}], received source doc [{}], indexed downsampled doc [{}], "
                + "skipped already indexed [{}], failed [{}], took [{}]",
            indexShard.shardId(),
            task.getNumReceived(),
            task.getNumSent(),
            task.getNumIndexed(),
            task.getNumSkippedAlreadyIndexed(),
            task.getNumFailed(),
            duration
        );

        if (abort || task.getNumIndexed() != task.getNumSent()) {
            // Either a bulk failed, during the scan or while the processor was closing, or closing the processor timed out while
            // bulks were still in flight. Both are handled the same way: the task state records the watermark, which counts the
            // unsettled bulks as unconfirmed, and the reassigned task resumes from it.
            downsampleMetrics.recordShardOperation(
                duration.millis(),
                task.getNumIndexed() != task.getNumSent()
                    ? DownsampleMetrics.ActionStatus.MISSING_DOCS
                    : DownsampleMetrics.ActionStatus.FAILED
            );
            throw bulkFailure();
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
        // The executor creates the state with the INITIALIZED status and the last tsid found in the target index, so the
        // presence of a tsid is what signals a resumed task, not the status.
        if (this.state.tsid() != null) {
            return XSortedSetDocValuesRangeQuery.newSlowRangeQuery(TimeSeriesIdFieldMapper.NAME, this.state.tsid(), null, true, false);
        }
        return Queries.ALL_DOCS_INSTANCE;
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
                new DownsampleShardPersistentTaskState(
                    DownsampleShardIndexerStatus.CANCELLED,
                    watermarkToPublish(),
                    state.targetIndexUuid()
                ),
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
            // Stop the scan. The failure is handled by execute() once the bulk processor is closed, see there.
            throw new DownsampleShardIndexerException("Bulk indexing failure", true);
        }
        maybePublishWatermark();
    }

    /**
     * The watermark to publish, or {@code null} when the state carries no target index UUID: such a watermark is never resumed
     * from, see {@link DownsampleShardPersistentTaskState#DOWNSAMPLE_RESUME_WATERMARK_TARGET_UUID}.
     */
    @Nullable
    private BytesRef watermarkToPublish() {
        return state.targetIndexUuid() == null ? null : resumeWatermark.current();
    }

    /**
     * Publishes the resume watermark in the task state while the scan runs, throttled by {@link ResumeWatermark}, so that a task
     * reassigned after a node loss resumes from it instead of from the beginning of the shard. The publication is not waited for.
     * It cannot do harm if it lands after the {@code FAILED} publication of a bulk failure: it carries a watermark that is at most
     * as high as the exact one, and a {@code STARTED} state only makes the waiting downsample request wait for the reassigned
     * task to finish rather than fail.
     */
    private void maybePublishWatermark() {
        if (state.targetIndexUuid() == null) {
            return;
        }
        BytesRef watermark = resumeWatermark.pollForPublication(client.threadPool().relativeTimeInMillis());
        if (watermark == null) {
            return;
        }
        ActionListener<PersistentTasksCustomMetadata.PersistentTask<?>> published = ActionListener.wrap(
            ignored -> resumeWatermark.publicationSucceeded(watermark, client.threadPool().relativeTimeInMillis()),
            e -> {
                logger.debug(() -> format("Shard [%s] failed to publish the downsample resume watermark", indexShard.shardId()), e);
                resumeWatermark.publicationFailed(client.threadPool().relativeTimeInMillis());
            }
        );
        // Run through the listener so that a synchronous failure of the update does not leave the publication in flight forever
        ActionListener.run(
            published,
            l -> task.updatePersistentTaskState(
                new DownsampleShardPersistentTaskState(DownsampleShardIndexerStatus.STARTED, watermark, state.targetIndexUuid()),
                l
            )
        );
    }

    /**
     * Publishes the {@code FAILED} state carrying the resume watermark and returns the retriable exception to throw, so that the
     * persistent task is reassigned and resumes from the watermark rather than from scratch. The publication is waited for: the
     * reassignment invalidates the task's allocation and a state update that lands after it is rejected. This is called once the
     * bulk processor is closed, so the watermark is exact, unless closing timed out, in which case the bulks still in flight are
     * unconfirmed and the watermark stops before them.
     */
    private DownsampleShardIndexerException bulkFailure() {
        logger.warn(
            "Shard [{}] downsample abort, sent [{}], indexed [{}], failed[{}]",
            indexShard.shardId(),
            task.getNumSent(),
            task.getNumIndexed(),
            task.getNumFailed()
        );
        task.setDownsampleShardIndexerStatus(DownsampleShardIndexerStatus.FAILED);
        PlainActionFuture<PersistentTasksCustomMetadata.PersistentTask<?>> published = new PlainActionFuture<>();
        ActionListener.run(
            published,
            l -> task.updatePersistentTaskState(
                new DownsampleShardPersistentTaskState(DownsampleShardIndexerStatus.FAILED, watermarkToPublish(), state.targetIndexUuid()),
                l
            )
        );
        try {
            published.actionGet(WATERMARK_PUBLISH_TIMEOUT);
        } catch (Exception e) {
            logger.warn(
                () -> format(
                    "Shard [%s] failed to publish the downsample resume watermark, the task will restart from scratch",
                    indexShard.shardId()
                ),
                e
            );
        }
        return new DownsampleShardIndexerException("Bulk indexing failure", true);
    }

    private BulkProcessor2 createBulkProcessor() {
        final BulkProcessor2.Listener listener = new BulkProcessor2.Listener() {

            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                task.addNumSent(request.numberOfActions());
                // Bulks are cut and sent by the scan thread while it adds a bucket, so this bulk holds exactly the buckets added
                // since the previous one was cut, and the bulks are registered in the order they are sent
                assert Thread.currentThread() == scanThread : "bulks must be cut by the thread scanning the shard";
                resumeWatermark.preparingBulkRequest(executionId);
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
                List<BulkItemResponse> failedItems = List.of();
                if (response.hasFailures()) {
                    // Version conflicts are expected when a task resumes after an interruption, the documents are created with op type
                    // CREATE, so the bucket that were already indexed by the previous run are rejected. They are skipped rather
                    // than counted as failures.
                    int conflicts = 0;
                    for (BulkItemResponse item : response.getItems()) {
                        if (item.isFailed() == false) {
                            continue;
                        }
                        if (item.getFailure().getStatus() == RestStatus.CONFLICT) {
                            conflicts++;
                        } else {
                            if (failedItems.isEmpty()) {
                                failedItems = new ArrayList<>();
                            }
                            failedItems.add(item);
                        }
                    }
                    if (conflicts > 0) {
                        task.addNumSkippedAlreadyIndexed(conflicts);
                        logger.debug(
                            "Shard [{}] skipped [{}] downsampled documents that were already indexed",
                            indexShard.shardId(),
                            conflicts
                        );
                    }
                }
                task.setAfterBulkInfo(
                    new DownsampleAfterBulkInfo(
                        client.threadPool().absoluteTimeInMillis(),
                        executionId,
                        bulkIngestTookMillis,
                        bulkTookMillis,
                        failedItems.isEmpty() == false,
                        RestStatus.OK.getStatus()
                    )
                );
                task.updateBulkInfo(bulkIngestTookMillis, bulkTookMillis);

                if (failedItems.isEmpty() == false) {
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
                } else {
                    resumeWatermark.succeeded(executionId);
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
        private static final NumericMetricFieldDownsampler.AggregateCounter[] EMPTY_AGGREGATE_COUNTERS =
            new NumericMetricFieldDownsampler.AggregateCounter[0];
        private final BulkProcessor2 bulkProcessor;
        private final DownsampleBucketBuilder downsampleBucketBuilder;
        private LeafDownsampleCollector currentLeafCollector;
        // Downsamplers grouped by the doc value input they expect, we use primitive arrays to reduce the footprint.
        private final DimensionFieldDownsampler[] dimensionDownsamplers;
        private final LastValueFieldDownsampler[] formattedDocValuesDownsamplers;
        private final ExponentialHistogramFieldDownsampler[] exponentialHistogramDownsamplers;
        private final TDigestHistogramFieldDownsampler[] tDigestHistogramDownsamplers;
        private final NumericMetricFieldDownsampler[] numericDownsamplers;
        // Aggregate counter and histogram downsamplers are dealt with separately because
        // they additionally require timestamps when temporality is cumulative.
        private final NumericMetricFieldDownsampler.AggregateCounter[] aggregateCounterDownsamplers;
        private final ExponentialHistogramFieldDownsampler.AggregateHistogram[] aggregateHistogramDownsamplers;
        private long docsProcessed;
        private long bucketsCreated;
        long lastTimestamp = Long.MAX_VALUE;
        long lastHistoTimestamp = Long.MAX_VALUE;

        /**
         * The array index of the dimension storing the temporality in {@link #dimensionDownsamplers}.
         * {@code -1} if there is no temporality dimension.
         */
        private final int temporalityDimensionIndex;

        TimeSeriesBucketCollector(BulkProcessor2 bulkProcessor, String[] dimensions) {
            this.bulkProcessor = bulkProcessor;
            int dimensionFieldIndex = 0;
            this.dimensionDownsamplers = new DimensionFieldDownsampler[fieldCounts.dimensionFields()];
            int numericFieldIndex = 0;
            this.numericDownsamplers = new NumericMetricFieldDownsampler[fieldCounts.numericFields()];
            int formattedValueFieldIndex = 0;
            this.formattedDocValuesDownsamplers = new LastValueFieldDownsampler[fieldCounts.formattedValueFields()];
            int exponentialHistogramFieldIndex = 0;
            this.exponentialHistogramDownsamplers = new ExponentialHistogramFieldDownsampler[fieldCounts
                .nonAggregateExponentialHistogramFields()];
            int tDigestHistogramFieldIndex = 0;
            this.tDigestHistogramDownsamplers = new TDigestHistogramFieldDownsampler[fieldCounts.tDigestHistogramFields()];
            int aggregateCounterFieldIndex = 0;
            this.aggregateCounterDownsamplers = fieldCounts.aggregateCounterFields() == 0
                ? EMPTY_AGGREGATE_COUNTERS
                : new NumericMetricFieldDownsampler.AggregateCounter[fieldCounts.aggregateCounterFields()];
            int aggregateHistogramFieldIndex = 0;
            this.aggregateHistogramDownsamplers = new ExponentialHistogramFieldDownsampler.AggregateHistogram[fieldCounts
                .aggregateExponentialHistogramFields()];
            for (AbstractFieldDownsampler<?> fieldDownsampler : fieldDownsamplers) {
                switch (fieldDownsampler) {
                    case NumericMetricFieldDownsampler.AggregateCounter aggregateCounter -> {
                        assert aggregateCounterFieldIndex < aggregateCounterDownsamplers.length;
                        aggregateCounterDownsamplers[aggregateCounterFieldIndex++] = aggregateCounter;
                    }
                    case NumericMetricFieldDownsampler numericMetricDownsampler -> {
                        assert numericFieldIndex < numericDownsamplers.length;
                        numericDownsamplers[numericFieldIndex++] = numericMetricDownsampler;
                    }
                    case DimensionFieldDownsampler dimensionDownsampler -> {
                        assert dimensionFieldIndex < dimensionDownsamplers.length;
                        dimensionDownsamplers[dimensionFieldIndex++] = dimensionDownsampler;
                    }
                    case LastValueFieldDownsampler lastValueDownsampler -> {
                        assert formattedValueFieldIndex < formattedDocValuesDownsamplers.length;
                        formattedDocValuesDownsamplers[formattedValueFieldIndex++] = lastValueDownsampler;
                    }
                    case ExponentialHistogramFieldDownsampler.AggregateHistogram aggregateHistogram -> {
                        assert aggregateHistogramFieldIndex < aggregateHistogramDownsamplers.length;
                        aggregateHistogramDownsamplers[aggregateHistogramFieldIndex++] = aggregateHistogram;
                    }
                    case ExponentialHistogramFieldDownsampler exponentialHistogramDownsampler -> {
                        assert exponentialHistogramFieldIndex < exponentialHistogramDownsamplers.length;
                        exponentialHistogramDownsamplers[exponentialHistogramFieldIndex++] = exponentialHistogramDownsampler;
                    }
                    case TDigestHistogramFieldDownsampler tDigestDownsampler -> {
                        assert tDigestHistogramFieldIndex < tDigestHistogramDownsamplers.length;
                        tDigestHistogramDownsamplers[tDigestHistogramFieldIndex++] = tDigestDownsampler;
                    }
                    default -> throw new IllegalArgumentException("Unknown field downsampler type: " + fieldDownsampler.getClass());
                }
            }

            int resolvedTemporalityIndex = -1;
            if (temporalityFieldName != null && temporalityFieldName.isEmpty() == false) {
                for (int i = 0; i < dimensionDownsamplers.length; i++) {
                    if (temporalityFieldName.equals(dimensionDownsamplers[i].name())) {
                        resolvedTemporalityIndex = i;
                        break;
                    }
                }
            }
            this.temporalityDimensionIndex = resolvedTemporalityIndex;

            this.downsampleBucketBuilder = new DownsampleBucketBuilder(
                fieldDownsamplers,
                aggregateCounterDownsamplers,
                aggregateHistogramDownsamplers,
                dimensionDownsamplers,
                dimensions
            );
        }

        @Override
        public LeafBucketCollector getLeafCollector(final AggregationExecutionContext aggCtx) throws IOException {
            final LeafReaderContext ctx = aggCtx.getLeafReaderContext();
            final DocCountProvider docCountProvider = new DocCountProvider();
            docCountProvider.setLeafReaderContext(ctx);

            // For each field retrieve the doc values for this segment
            var numericValues = new SortedNumericDoubleValues[numericDownsamplers.length];
            for (int i = 0; i < numericDownsamplers.length; i++) {
                numericValues[i] = numericDownsamplers[i].getLeaf(ctx);
            }
            var formattedDocValues = new FormattedDocValues[formattedDocValuesDownsamplers.length];
            for (int i = 0; i < formattedDocValuesDownsamplers.length; i++) {
                formattedDocValues[i] = formattedDocValuesDownsamplers[i].getLeaf(ctx);
            }
            var dimensionDocValues = new FormattedDocValues[dimensionDownsamplers.length];
            for (int i = 0; i < dimensionDownsamplers.length; i++) {
                dimensionDocValues[i] = dimensionDownsamplers[i].getLeaf(ctx);
            }
            var exponentialHistogramValues = new ExponentialHistogramValuesReader[exponentialHistogramDownsamplers.length];
            for (int i = 0; i < exponentialHistogramDownsamplers.length; i++) {
                exponentialHistogramValues[i] = exponentialHistogramDownsamplers[i].getLeaf(ctx);
            }
            var tDigestHistogramValues = new HistogramValues[tDigestHistogramDownsamplers.length];
            for (int i = 0; i < tDigestHistogramDownsamplers.length; i++) {
                tDigestHistogramValues[i] = tDigestHistogramDownsamplers[i].getLeaf(ctx);
            }
            var aggregateCounterValues = new SortedNumericDoubleValues[aggregateCounterDownsamplers.length];
            for (int i = 0; i < aggregateCounterDownsamplers.length; i++) {
                aggregateCounterValues[i] = aggregateCounterDownsamplers[i].getLeaf(ctx);
            }
            var aggregateHistogramValues = new ExponentialHistogramValuesReader[aggregateHistogramDownsamplers.length];
            for (int i = 0; i < aggregateHistogramDownsamplers.length; i++) {
                aggregateHistogramValues[i] = aggregateHistogramDownsamplers[i].getLeaf(ctx);
            }
            boolean needTimestamps = aggregateCounterDownsamplers.length > 0 || aggregateHistogramDownsamplers.length > 0;
            var timestampValues = needTimestamps ? timestampValueFetcher.getLeaf(ctx) : null;

            return new LeafDownsampleCollector(
                aggCtx,
                docCountProvider,
                dimensionDocValues,
                numericValues,
                formattedDocValues,
                exponentialHistogramValues,
                tDigestHistogramValues,
                aggregateCounterValues,
                aggregateHistogramValues,
                timestampValues
            );
        }

        void bulkCollection() throws IOException {
            if (currentLeafCollector != null) {
                currentLeafCollector.leafBulkCollection();
            }
        }

        class LeafDownsampleCollector extends LeafBucketCollector {

            final AggregationExecutionContext aggCtx;
            final DocCountProvider docCountProvider;
            final FormattedDocValues[] dimensionDocValues;
            final SortedNumericDoubleValues[] numericValues;
            final FormattedDocValues[] formattedDocValues;
            final ExponentialHistogramValuesReader[] exponentialHistogramValues;
            final HistogramValues[] tDigestHistogramValues;
            private final SortedNumericDoubleValues[] aggregateCounterValues;
            private final ExponentialHistogramValuesReader[] aggregateHistogramValues;
            final SortedNumericLongValues timestampValues;

            final IntArrayList docIdBuffer = new IntArrayList(DOCID_BUFFER_SIZE);
            final LongArrayList timestampBuffer = new LongArrayList(DOCID_BUFFER_SIZE);
            final long timestampBoundStartTime = searchExecutionContext.getIndexSettings().getTimestampBounds().startTime();

            LeafDownsampleCollector(
                AggregationExecutionContext aggCtx,
                DocCountProvider docCountProvider,
                FormattedDocValues[] dimensionDocValues,
                SortedNumericDoubleValues[] numericValues,
                FormattedDocValues[] formattedDocValues,
                ExponentialHistogramValuesReader[] exponentialHistogramValues,
                HistogramValues[] tDigestHistogramValues,
                SortedNumericDoubleValues[] aggregateCounterValues,
                ExponentialHistogramValuesReader[] aggregateHistogramValues,
                SortedNumericLongValues timestampValues
            ) {
                this.aggCtx = aggCtx;
                this.docCountProvider = docCountProvider;
                this.dimensionDocValues = dimensionDocValues;
                this.numericValues = numericValues;
                this.formattedDocValues = formattedDocValues;
                this.exponentialHistogramValues = exponentialHistogramValues;
                this.tDigestHistogramValues = tDigestHistogramValues;
                this.aggregateCounterValues = aggregateCounterValues;
                this.aggregateHistogramValues = aggregateHistogramValues;
                this.timestampValues = timestampValues;
            }

            @Override
            public void collect(int docId, long owningBucketOrd) throws IOException {
                if (currentLeafCollector != this) {
                    bulkCollection();
                    currentLeafCollector = this;
                }
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
                    flushIfNotEmpty();

                    // Create new downsample bucket
                    if (tsidChanged) {
                        downsampleBucketBuilder.resetTsid(tsidHash, tsidHashOrd, lastHistoTimestamp);
                    } else {
                        downsampleBucketBuilder.resetTimestamp(lastHistoTimestamp);
                    }
                    bucketsCreated++;
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
                collect(numericDownsamplers, numericValues);
                collect(formattedDocValuesDownsamplers, formattedDocValues);
                collect(exponentialHistogramDownsamplers, exponentialHistogramValues);
                collect(tDigestHistogramDownsamplers, tDigestHistogramValues);
                if (downsampleBucketBuilder.dimensionsCollected == false) {
                    assert dimensionDownsamplers.length == dimensionDocValues.length
                        : "Number of downsamplers ["
                            + dimensionDownsamplers.length
                            + "] does not match number of doc values ["
                            + dimensionDocValues.length
                            + "]";
                    for (int i = 0; i < dimensionDownsamplers.length; i++) {
                        dimensionDownsamplers[i].collectOnce(dimensionDocValues[i], docIdBuffer);
                    }
                    downsampleBucketBuilder.dimensionsCollected = true;
                }
                Temporality temporality = Temporality.DEFAULT;
                if (temporalityDimensionIndex != -1) {
                    temporality = Temporality.fromDimensionValue(dimensionDownsamplers[temporalityDimensionIndex].dimensionValue());
                }
                if (aggregateCounterDownsamplers.length > 0 || aggregateHistogramDownsamplers.length > 0) {
                    assert timestampValues != null;
                    TimestampValueFetcher.fetch(timestampValues, docIdBuffer, timestampBuffer);
                    for (int i = 0; i < aggregateCounterDownsamplers.length; i++) {
                        aggregateCounterDownsamplers[i].collect(aggregateCounterValues[i], timestampBuffer, docIdBuffer, temporality);
                    }
                    for (int i = 0; i < aggregateHistogramDownsamplers.length; i++) {
                        aggregateHistogramDownsamplers[i].collect(aggregateHistogramValues[i], timestampBuffer, docIdBuffer, temporality);
                    }
                }

                docsProcessed += docIdBuffer.size();
                task.setDocsProcessed(docsProcessed);

                // buffer.clean() also overwrites all slots with zeros
                docIdBuffer.elementsCount = 0;
                timestampBuffer.elementsCount = 0;
            }

            private <T> void collect(AbstractFieldDownsampler<T>[] downsamplers, T[] docValues) throws IOException {
                assert downsamplers.length == docValues.length
                    : "Number of downsamplers [" + downsamplers.length + "] does not match number of doc values [" + docValues.length + "]";
                for (int i = 0; i < downsamplers.length; i++) {
                    downsamplers[i].collect(docValues[i], docIdBuffer);
                }
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

        private void flushIfNotEmpty() throws IOException {
            if (downsampleBucketBuilder.isEmpty() == false) {
                downsampleBucketBuilder.updateResetDataPoints();
                XContentBuilder downsampleDocument = downsampleBucketBuilder.buildDownsampleDocument();
                indexBucket(downsampleDocument);

                downsampleBucketBuilder.flushResetDocumentsIfNeeded(this::indexBucket);
            }
        }

        private void indexBucket(XContentBuilder doc) {
            IndexRequestBuilder request = client.prepareIndex(downsampleIndex);
            // The _id is derived from _tsid and timestamp. Use CREATE so that a bucket already indexed by a previous,
            // interrupted run of this task is rejected with a version conflict instead of being indexed a second time.
            request.setOpType(DocWriteRequest.OpType.CREATE);
            request.setSource(doc);
            if (logger.isTraceEnabled()) {
                logger.trace("Indexing downsample doc: [{}]", Strings.toString(doc));
            }
            IndexRequest indexRequest = request.request();
            task.setLastIndexingTimestamp(System.currentTimeMillis());
            // Reported before the add, because the add may cut a bulk and register it with the watermark. If the add is rejected
            // instead, the watermark names a bucket that is in no bulk, which only makes a resumed task re-read it.
            resumeWatermark.addedTsid(downsampleBucketBuilder.tsid());
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
            flushIfNotEmpty();

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
        private ResetDataPoints resetDataPoints;
        private boolean dimensionsCollected = false;
        // A list of all the downsamplers so we can reset them before moving on to the next bucket
        private final List<AbstractFieldDownsampler<?>> fieldDownsamplers;
        // An array of field serializers, each field has one serializer which can group one or more AbstractFieldDownsamplers
        private final DownsampleFieldSerializer[] fieldSerializers;
        private final DimensionFieldDownsampler[] dimensionDownsamplers;
        private final NumericMetricFieldDownsampler.AggregateCounter[] aggregateCounterDownsamplers;
        private final ExponentialHistogramFieldDownsampler.AggregateHistogram[] aggregateHistogramDownsamplers;
        private final boolean legacyDimensions;

        DownsampleBucketBuilder(
            List<AbstractFieldDownsampler<?>> fieldDownsamplers,
            NumericMetricFieldDownsampler.AggregateCounter[] aggregateCounterDownsamplers,
            ExponentialHistogramFieldDownsampler.AggregateHistogram[] aggregateHistogramDownsamplers,
            DimensionFieldDownsampler[] dimensionDownsamplers,
            String[] dimensions
        ) {
            this.fieldDownsamplers = fieldDownsamplers;
            this.legacyDimensions = dimensions.length == 0;
            this.dimensionDownsamplers = dimensionDownsamplers;
            this.aggregateCounterDownsamplers = aggregateCounterDownsamplers;
            this.aggregateHistogramDownsamplers = aggregateHistogramDownsamplers;
            /*
             * The field downsamplers for aggregate_metric_double all share the same name (this is
             * the name they will be serialized in the target index). We group all field downsamplers by
             * name. If grouping yields multiple field downsamplers, we delegate serialization to
             * the AggregateMetricFieldSerializer class.
             */
            fieldSerializers = fieldDownsamplers.stream().collect(groupingBy(AbstractFieldDownsampler::name)).entrySet().stream().map(e -> {
                if (e.getValue().size() == 1) {
                    return e.getValue().get(0);
                } else {
                    return new AggregateMetricDoubleFieldDownsampler.Serializer(e.getKey(), e.getValue());
                }
            }).toArray(DownsampleFieldSerializer[]::new);
        }

        /**
         * tsid changed, reset tsid and timestamp
         */
        public void resetTsid(BytesRef tsid, int tsidOrd, long timestamp) {
            this.tsid = BytesRef.deepCopyOf(tsid);
            this.tsidOrd = tsidOrd;
            resetTimestamp(timestamp);
            // In case of tsid change, the aggregate counter downsamplers need to reset the previous value
            for (int i = 0; i < aggregateCounterDownsamplers.length; i++) {
                aggregateCounterDownsamplers[i].tsidReset();
            }
            for (int i = 0; i < aggregateHistogramDownsamplers.length; i++) {
                aggregateHistogramDownsamplers[i].tsidReset();
            }
            // Reset dimension downsamplers
            for (int i = 0; i < dimensionDownsamplers.length; i++) {
                dimensionDownsamplers[i].tsidReset();
            }
            dimensionsCollected = false;
        }

        /**
         * timestamp change, reset builder
         */
        public void resetTimestamp(long timestamp) {
            this.timestamp = timestamp;
            this.docCount = 0;
            for (AbstractFieldDownsampler<?> downsampler : fieldDownsamplers) {
                downsampler.reset();
            }
            boolean needResetTracking = aggregateCounterDownsamplers.length > 0 || aggregateHistogramDownsamplers.length > 0;
            this.resetDataPoints = needResetTracking ? new ResetDataPoints() : null;
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

        public void updateResetDataPoints() {
            if (resetDataPoints != null) {
                for (int i = 0; i < aggregateCounterDownsamplers.length; i++) {
                    aggregateCounterDownsamplers[i].updateResetDataPoints(resetDataPoints);
                }
                for (int i = 0; i < aggregateHistogramDownsamplers.length; i++) {
                    aggregateHistogramDownsamplers[i].updateResetDataPoints(resetDataPoints);
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
            // We remove the reset documents from the doc count otherwise in every downsample round
            // the doc count will re-count the reset documents.
            int resetDocCount = resetDataPoints == null ? 0 : resetDataPoints.countResetDocuments();
            int downsampledDocumentDocCount = docCount - resetDocCount;
            assert downsampledDocumentDocCount > 0 : "Reset documents should already be included in the processed document count";
            builder.field(DocCountFieldMapper.NAME, downsampledDocumentDocCount);

            // Serialize fields
            for (DownsampleFieldSerializer fieldDownsampler : fieldSerializers) {
                fieldDownsampler.write(builder);
            }

            extractLegacyDimensionsIfNeeded(builder);

            builder.endObject();
            return builder;
        }

        public XContentBuilder buildExtraResetDocument(long timestamp, Map<String, ResetDataPoints.ResetValue> resetValues)
            throws IOException {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.SMILE);
            builder.startObject();
            builder.field(timestampField.name(), timestampFormat.format(timestamp));

            for (DimensionFieldDownsampler dimensionFieldDownsampler : dimensionDownsamplers) {
                dimensionFieldDownsampler.write(builder);
            }
            for (var resetValue : resetValues.entrySet()) {
                resetValue.getValue().write(resetValue.getKey(), builder);
            }

            extractLegacyDimensionsIfNeeded(builder);

            builder.endObject();
            return builder;
        }

        void flushResetDocumentsIfNeeded(Consumer<XContentBuilder> indexResetDoc) throws IOException {
            if (resetDataPoints != null && resetDataPoints.isEmpty() == false) {
                AtomicReference<IOException> error = new AtomicReference<>();
                resetDataPoints.processDataPoints((timestamp, resetValues) -> {
                    try {
                        XContentBuilder resetDoc = buildExtraResetDocument(timestamp, resetValues);
                        indexResetDoc.accept(resetDoc);
                    } catch (IOException e) {
                        // buffer the error and continue with the remaining timestamps / documents
                        error.set(e);
                    }
                });
                if (error.get() != null) {
                    throw error.get();
                }
            }
        }

        private void extractLegacyDimensionsIfNeeded(XContentBuilder builder) throws IOException {
            if (legacyDimensions) {
                logger.debug("extracting dimensions from legacy tsid");
                Map<?, ?> dimensions = (Map<?, ?>) DocValueFormat.TIME_SERIES_ID.format(tsid);
                for (Map.Entry<?, ?> e : dimensions.entrySet()) {
                    assert e.getValue() != null;
                    builder.field((String) e.getKey(), e.getValue());
                }
            }
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
