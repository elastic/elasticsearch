/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ElasticsearchWrapperException;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentElasticsearchExtension;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.action.FlushJobAction;
import org.elasticsearch.xpack.core.ml.action.PersistJobAction;
import org.elasticsearch.xpack.core.ml.action.PostDataAction;
import org.elasticsearch.xpack.core.ml.annotations.Annotation;
import org.elasticsearch.xpack.core.ml.datafeed.SearchInterval;
import org.elasticsearch.xpack.core.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.ml.annotations.AnnotationPersister;
import org.elasticsearch.xpack.ml.datafeed.delayeddatacheck.DelayedDataDetector;
import org.elasticsearch.xpack.ml.datafeed.delayeddatacheck.DelayedDataDetectorFactory.BucketWithMissingData;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

class DatafeedJob {

    private static final Logger LOGGER = LogManager.getLogger(DatafeedJob.class);
    private static final int NEXT_TASK_DELAY_MS = 100;

    private final AnomalyDetectionAuditor auditor;
    private final AnnotationPersister annotationPersister;
    private final String jobId;
    private final DataDescription dataDescription;
    private final long frequencyMs;
    private final long queryDelayMs;
    private final Client client;
    private final DataExtractorFactory dataExtractorFactory;
    private final DatafeedTimingStatsReporter timingStatsReporter;
    private final Supplier<Long> currentTimeSupplier;
    private final DelayedDataDetector delayedDataDetector;
    private final Integer maxEmptySearches;
    private final long delayedDataCheckFreq;

    private volatile long lookbackStartTimeMs;
    private volatile long latestFinalBucketEndTimeMs;
    private volatile long lastDataCheckTimeMs;
    private volatile Tuple<String, Annotation> lastDataCheckAnnotationWithId;
    private volatile Long lastEndTimeMs;
    private AtomicBoolean running = new AtomicBoolean(true);
    private volatile boolean isIsolated;
    private volatile boolean haveEverSeenData;
    private volatile long consecutiveDelayedDataBuckets;
    private volatile SearchInterval searchInterval;

    DatafeedJob(
        String jobId,
        DataDescription dataDescription,
        long frequencyMs,
        long queryDelayMs,
        DataExtractorFactory dataExtractorFactory,
        DatafeedTimingStatsReporter timingStatsReporter,
        Client client,
        AnomalyDetectionAuditor auditor,
        AnnotationPersister annotationPersister,
        Supplier<Long> currentTimeSupplier,
        DelayedDataDetector delayedDataDetector,
        Integer maxEmptySearches,
        long latestFinalBucketEndTimeMs,
        long latestRecordTimeMs,
        boolean haveSeenDataPreviously,
        long delayedDataCheckFreq
    ) {
        this.jobId = jobId;
        this.dataDescription = Objects.requireNonNull(dataDescription);
        this.frequencyMs = frequencyMs;
        this.queryDelayMs = queryDelayMs;
        this.dataExtractorFactory = dataExtractorFactory;
        this.timingStatsReporter = timingStatsReporter;
        this.client = client;
        this.auditor = auditor;
        this.annotationPersister = annotationPersister;
        this.currentTimeSupplier = currentTimeSupplier;
        this.delayedDataDetector = delayedDataDetector;
        this.maxEmptySearches = maxEmptySearches;
        this.latestFinalBucketEndTimeMs = latestFinalBucketEndTimeMs;
        long lastEndTime = Math.max(latestFinalBucketEndTimeMs, latestRecordTimeMs);
        if (lastEndTime > 0) {
            lastEndTimeMs = lastEndTime;
        }
        this.haveEverSeenData = haveSeenDataPreviously;
        this.delayedDataCheckFreq = delayedDataCheckFreq;
    }

    void isolate() {
        isIsolated = true;
        timingStatsReporter.disallowPersisting();
    }

    boolean isIsolated() {
        return isIsolated;
    }

    public String getJobId() {
        return jobId;
    }

    public Integer getMaxEmptySearches() {
        return maxEmptySearches;
    }

    public void finishReportingTimingStats() {
        timingStatsReporter.finishReporting();
    }

    @Nullable
    public SearchInterval getSearchInterval() {
        return searchInterval;
    }

    Long runLookBack(long startTime, Long endTime) throws Exception {
        lookbackStartTimeMs = skipToStartTime(startTime);
        Optional<Long> endMs = Optional.ofNullable(endTime);
        long lookbackEnd = endMs.orElse(currentTimeSupplier.get() - queryDelayMs);
        boolean isLookbackOnly = endMs.isPresent();
        if (lookbackEnd <= lookbackStartTimeMs) {
            if (isLookbackOnly) {
                return null;
            } else {
                auditor.info(jobId, Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_STARTED_REALTIME));
                return nextRealtimeTimestamp();
            }
        }

        String msg = Messages.getMessage(
            Messages.JOB_AUDIT_DATAFEED_STARTED_FROM_TO,
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(lookbackStartTimeMs),
            endTime == null ? "real-time" : DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(lookbackEnd),
            TimeValue.timeValueMillis(frequencyMs).getStringRep()
        );
        auditor.info(jobId, msg);
        LOGGER.info("[{}] {}", jobId, msg);

        FlushJobAction.Request request = new FlushJobAction.Request(jobId);
        request.setCalcInterim(true);
        run(lookbackStartTimeMs, lookbackEnd, request);
        if (shouldPersistAfterLookback(isLookbackOnly)) {
            sendPersistRequest();
        }

        if (isRunning() && isIsolated == false) {
            auditor.info(jobId, Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_LOOKBACK_COMPLETED));
            LOGGER.info("[{}] Lookback has finished", jobId);
            if (isLookbackOnly) {
                return null;
            } else {
                auditor.info(jobId, Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_CONTINUED_REALTIME));
                return nextRealtimeTimestamp();
            }
        }
        if (isIsolated == false) {
            LOGGER.debug("[{}] Lookback finished after being stopped", jobId);
        }
        return null;
    }

    private long skipToStartTime(long startTime) {
        if (lastEndTimeMs != null) {
            if (lastEndTimeMs + 1 > startTime) {
                // start time is before last checkpoint, thus continue from checkpoint
                return lastEndTimeMs + 1;
            }
            // start time is after last checkpoint, thus we need to skip time
            FlushJobAction.Request request = new FlushJobAction.Request(jobId);
            request.setSkipTime(String.valueOf(startTime));
            FlushJobAction.Response flushResponse = flushJob(request);
            LOGGER.info("[{}] Skipped to time [{}]", jobId, flushResponse.getLastFinalizedBucketEnd().toEpochMilli());
            return flushResponse.getLastFinalizedBucketEnd().toEpochMilli();
        }
        return startTime;
    }

    long runRealtime() throws Exception {
        long start = lastEndTimeMs == null ? lookbackStartTimeMs : Math.max(lookbackStartTimeMs, lastEndTimeMs + 1);
        long nowMinusQueryDelay = currentTimeSupplier.get() - queryDelayMs;
        long end = toIntervalStartEpochMs(nowMinusQueryDelay);
        FlushJobAction.Request request = new FlushJobAction.Request(jobId);
        request.setWaitForNormalization(false);
        request.setCalcInterim(true);
        request.setAdvanceTime(String.valueOf(end));
        run(start, end, request);
        checkForMissingDataIfNecessary();
        return nextRealtimeTimestamp();
    }

    private void checkForMissingDataIfNecessary() {
        if (isRunning() && isIsolated == false && checkForMissingDataTriggered()) {

            // Keep track of the last bucket time for which we did a missing data check
            this.lastDataCheckTimeMs = this.currentTimeSupplier.get();
            List<BucketWithMissingData> missingDataBuckets = delayedDataDetector.detectMissingData(latestFinalBucketEndTimeMs);
            if (missingDataBuckets.isEmpty() == false) {
                long totalRecordsMissing = missingDataBuckets.stream().mapToLong(BucketWithMissingData::getMissingDocumentCount).sum();
                Bucket lastBucket = missingDataBuckets.get(missingDataBuckets.size() - 1).getBucket();
                // Get the end of the last bucket and make it milliseconds
                Date endTime = new Date((lastBucket.getEpoch() + lastBucket.getBucketSpan()) * 1000);

                String msg = Messages.getMessage(
                    Messages.JOB_AUDIT_DATAFEED_MISSING_DATA,
                    totalRecordsMissing,
                    XContentElasticsearchExtension.DEFAULT_FORMATTER.format(lastBucket.getTimestamp().toInstant())
                );

                Annotation annotation = createDelayedDataAnnotation(missingDataBuckets.get(0).getBucket().getTimestamp(), endTime, msg);

                // Have we an annotation that covers the same area with the same message?
                // Cannot use annotation.equals(other) as that checks createTime
                if (lastDataCheckAnnotationWithId != null
                    && annotation.getAnnotation().equals(lastDataCheckAnnotationWithId.v2().getAnnotation())
                    && annotation.getTimestamp().equals(lastDataCheckAnnotationWithId.v2().getTimestamp())
                    && annotation.getEndTimestamp().equals(lastDataCheckAnnotationWithId.v2().getEndTimestamp())) {
                    return;
                }
                if (lastDataCheckAnnotationWithId != null) {
                    // NOTE: this check takes advantage of the following:
                    // * Bucket span is constant
                    // * The endtime has changed since our previous annotation
                    // * DatafeedJob objects only ever move forward in time
                    // All that to say, checking if the lastBucket overlaps the previous annotation end-time, that means that this current
                    // bucket with missing data is consecutive to the previous one.
                    if (lastBucket.getEpoch() * 1000 <= (lastDataCheckAnnotationWithId.v2().getEndTimestamp().getTime() + 1)) {
                        consecutiveDelayedDataBuckets++;
                    } else {
                        consecutiveDelayedDataBuckets = 0;
                    }
                } else {
                    consecutiveDelayedDataBuckets = 0;
                }
                // to prevent audit log spam on many consecutive buckets missing data, we should throttle writing the messages
                if (shouldWriteDelayedDataAudit()) {
                    // Creating a warning in addition to updating/creating our annotation. This allows the issue to be plainly visible
                    // in the job list page.
                    auditor.warning(jobId, msg);
                }

                if (lastDataCheckAnnotationWithId == null) {
                    lastDataCheckAnnotationWithId = annotationPersister.persistAnnotation(null, annotation);
                } else {
                    String annotationId = lastDataCheckAnnotationWithId.v1();
                    Annotation updatedAnnotation = updateAnnotation(annotation);
                    lastDataCheckAnnotationWithId = annotationPersister.persistAnnotation(annotationId, updatedAnnotation);
                }
            }
        }
    }

    private boolean shouldWriteDelayedDataAudit() {
        if (consecutiveDelayedDataBuckets < 3) {
            return true;
        }
        if (consecutiveDelayedDataBuckets < 100) {
            return consecutiveDelayedDataBuckets % 10 == 0;
        }
        return consecutiveDelayedDataBuckets % 100 == 0;
    }

    private Annotation createDelayedDataAnnotation(Date startTime, Date endTime, String msg) {
        Date currentTime = new Date(currentTimeSupplier.get());
        return new Annotation.Builder().setAnnotation(msg)
            .setCreateTime(currentTime)
            .setCreateUsername(XPackUser.NAME)
            .setTimestamp(startTime)
            .setEndTimestamp(endTime)
            .setJobId(jobId)
            .setModifiedTime(currentTime)
            .setModifiedUsername(XPackUser.NAME)
            .setType(Annotation.Type.ANNOTATION)
            .setEvent(Annotation.Event.DELAYED_DATA)
            .build();
    }

    private Annotation updateAnnotation(Annotation annotation) {
        return new Annotation.Builder(lastDataCheckAnnotationWithId.v2()).setAnnotation(annotation.getAnnotation())
            .setTimestamp(annotation.getTimestamp())
            .setEndTimestamp(annotation.getEndTimestamp())
            .setModifiedTime(new Date(currentTimeSupplier.get()))
            .setModifiedUsername(XPackUser.NAME)
            .build();
    }

    /**
     * We wait for `delayedDataCheckFreq` interval till the next missing data check.
     *
     * However, if our delayed data window is smaller than that, we will probably want to check at every available window (if freq. allows).
     * This is to help to miss as few buckets in the delayed data check as possible.
     *
     * If our frequency/query delay are longer then our default interval or window size, we will end up looking for missing data on
     * every real-time trigger. This should be OK as the we are pulling from the Index as such a slow pace, another query will
     * probably not even be noticeable at such a large timescale.
     */
    private boolean checkForMissingDataTriggered() {
        return this.currentTimeSupplier.get() > this.lastDataCheckTimeMs + Math.min(delayedDataCheckFreq, delayedDataDetector.getWindow());
    }

    /**
     * Stops the datafeed job
     *
     * @return <code>true</code> when the datafeed was running and this method invocation stopped it,
     *         otherwise <code>false</code> is returned
     */
    public boolean stop() {
        return running.compareAndSet(true, false);
    }

    public boolean isRunning() {
        return running.get();
    }

    private void run(long start, long end, FlushJobAction.Request flushRequest) throws IOException {
        if (end <= start) {
            return;
        }

        LOGGER.trace("[{}] Searching data in: [{}, {})", jobId, start, end);

        // A storage for errors that should only be thrown after advancing time
        RuntimeException error = null;

        long recordCount = 0;
        DataExtractor dataExtractor = dataExtractorFactory.newExtractor(start, end);
        while (dataExtractor.hasNext()) {
            if ((isIsolated || isRunning() == false) && dataExtractor.isCancelled() == false) {
                dataExtractor.cancel();
            }
            if (isIsolated) {
                return;
            }

            Optional<InputStream> extractedData;
            try {
                DataExtractor.Result result = dataExtractor.next();
                extractedData = result.data();
                searchInterval = result.searchInterval();
            } catch (Exception e) {
                LOGGER.error(new ParameterizedMessage("[{}] error while extracting data", jobId), e);
                // When extraction problems are encountered, we do not want to advance time.
                // Instead, it is preferable to retry the given interval next time an extraction
                // is triggered.

                // For aggregated datafeeds it is possible for our users to use fields without doc values.
                // In that case, it is really useful to display an error message explaining exactly that.
                // Unfortunately, there are no great ways to identify the issue but search for 'doc values'
                // deep in the exception.
                if (e.toString().contains("doc values")) {
                    throw new ExtractionProblemException(
                        nextRealtimeTimestamp(),
                        new IllegalArgumentException(
                            "One or more fields do not have doc values; please enable doc values for all analysis fields for datafeeds"
                                + " using aggregations"
                        )
                    );
                }
                throw new ExtractionProblemException(nextRealtimeTimestamp(), e);
            }
            if (isIsolated) {
                return;
            }
            if (extractedData.isPresent()) {
                DataCounts counts;
                try (InputStream in = extractedData.get()) {
                    counts = postData(in, XContentType.JSON);
                    LOGGER.trace(
                        () -> new ParameterizedMessage(
                            "[{}] Processed another {} records with latest timestamp [{}]",
                            jobId,
                            counts.getProcessedRecordCount(),
                            counts.getLatestRecordTimeStamp()
                        )
                    );
                    timingStatsReporter.reportDataCounts(counts);
                } catch (Exception e) {
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    if (isIsolated) {
                        return;
                    }
                    LOGGER.error(new ParameterizedMessage("[{}] error while posting data", jobId), e);

                    // a conflict exception means the job state is not open any more.
                    // we should therefore stop the datafeed.
                    boolean shouldStop = isConflictException(e);

                    // When an analysis problem occurs, it means something catastrophic has
                    // happened to the c++ process. We sent a batch of data to the c++ process
                    // yet we do not know how many of those were processed. It is better to
                    // advance time in order to avoid importing duplicate data.
                    error = new AnalysisProblemException(nextRealtimeTimestamp(), shouldStop, e);
                    break;
                }
                recordCount += counts.getProcessedRecordCount();
                haveEverSeenData |= (recordCount > 0);
                if (counts.getLatestRecordTimeStamp() != null) {
                    lastEndTimeMs = counts.getLatestRecordTimeStamp().getTime();
                }
            }
        }

        lastEndTimeMs = Math.max(lastEndTimeMs == null ? 0 : lastEndTimeMs, dataExtractor.getEndTime() - 1);
        LOGGER.debug(
            "[{}] Complete iterating data extractor [{}], [{}], [{}], [{}], [{}]",
            jobId,
            error,
            recordCount,
            lastEndTimeMs,
            isRunning(),
            dataExtractor.isCancelled()
        );

        // We can now throw any stored error as we have updated time.
        if (error != null) {
            throw error;
        }

        // If the datafeed was stopped, then it is possible that by the time
        // we call flush the job is closed. Thus, we don't flush unless the
        // datafeed is still running.
        if (isRunning() && isIsolated == false) {
            Instant lastFinalizedBucketEnd = flushJob(flushRequest).getLastFinalizedBucketEnd();
            if (lastFinalizedBucketEnd != null) {
                this.latestFinalBucketEndTimeMs = lastFinalizedBucketEnd.toEpochMilli();
            }
        }

        if (recordCount == 0) {
            throw new EmptyDataCountException(nextRealtimeTimestamp(), haveEverSeenData);
        }
    }

    private DataCounts postData(InputStream inputStream, XContentType xContentType) throws IOException {
        PostDataAction.Request request = new PostDataAction.Request(jobId);
        request.setDataDescription(dataDescription);
        request.setContent(Streams.readFully(inputStream), xContentType);
        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(ML_ORIGIN)) {
            PostDataAction.Response response = client.execute(PostDataAction.INSTANCE, request).actionGet();
            return response.getDataCounts();
        }
    }

    private boolean isConflictException(Exception e) {
        return e instanceof ElasticsearchStatusException && ((ElasticsearchStatusException) e).status() == RestStatus.CONFLICT;
    }

    private long nextRealtimeTimestamp() {
        // We find the timestamp of the start of the next frequency interval.
        // The goal is to minimize any lag. To do so,
        // we offset the time by the query delay modulo frequency.
        // For example, if frequency is 60s and query delay 90s,
        // we run 30s past the minute. If frequency is 1s and query delay 10s,
        // we don't add anything and we'll run every second.
        long next = currentTimeSupplier.get() + frequencyMs;
        return toIntervalStartEpochMs(next) + queryDelayMs % frequencyMs + NEXT_TASK_DELAY_MS;
    }

    private long toIntervalStartEpochMs(long epochMs) {
        return (epochMs / frequencyMs) * frequencyMs;
    }

    private FlushJobAction.Response flushJob(FlushJobAction.Request flushRequest) {
        try {
            LOGGER.trace("[" + jobId + "] Sending flush request");
            try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(ML_ORIGIN)) {
                return client.execute(FlushJobAction.INSTANCE, flushRequest).actionGet();
            }
        } catch (Exception e) {
            LOGGER.debug("[" + jobId + "] error while flushing job", e);

            // a conflict exception means the job state is not open any more.
            // we should therefore stop the datafeed.
            boolean shouldStop = isConflictException(e);

            // When an analysis problem occurs, it means something catastrophic has
            // happened to the c++ process. We sent a batch of data to the c++ process
            // yet we do not know how many of those were processed. It is better to
            // advance time in order to avoid importing duplicate data.
            throw new AnalysisProblemException(nextRealtimeTimestamp(), shouldStop, e);
        }
    }

    private boolean shouldPersistAfterLookback(boolean isLookbackOnly) {
        return isLookbackOnly == false && isIsolated == false && isRunning();
    }

    private void sendPersistRequest() {
        try {
            LOGGER.trace("[" + jobId + "] Sending persist request");
            try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(ML_ORIGIN)) {
                client.execute(PersistJobAction.INSTANCE, new PersistJobAction.Request(jobId));
            }
        } catch (Exception e) {
            LOGGER.debug("[" + jobId + "] error while persisting job", e);
        }
    }

    /**
     * Visible for testing
     */
    Long lastEndTimeMs() {
        return lastEndTimeMs;
    }

    static class AnalysisProblemException extends ElasticsearchException implements ElasticsearchWrapperException {

        final boolean shouldStop;
        final long nextDelayInMsSinceEpoch;

        AnalysisProblemException(long nextDelayInMsSinceEpoch, boolean shouldStop, Throwable cause) {
            super(cause);
            this.shouldStop = shouldStop;
            this.nextDelayInMsSinceEpoch = nextDelayInMsSinceEpoch;
        }
    }

    static class ExtractionProblemException extends ElasticsearchException implements ElasticsearchWrapperException {

        final long nextDelayInMsSinceEpoch;

        ExtractionProblemException(long nextDelayInMsSinceEpoch, Throwable cause) {
            super(cause);
            this.nextDelayInMsSinceEpoch = nextDelayInMsSinceEpoch;
        }
    }

    static class EmptyDataCountException extends RuntimeException {

        final long nextDelayInMsSinceEpoch;
        final boolean haveEverSeenData;

        EmptyDataCountException(long nextDelayInMsSinceEpoch, boolean haveEverSeenData) {
            this.nextDelayInMsSinceEpoch = nextDelayInMsSinceEpoch;
            this.haveEverSeenData = haveEverSeenData;
        }
    }
}
