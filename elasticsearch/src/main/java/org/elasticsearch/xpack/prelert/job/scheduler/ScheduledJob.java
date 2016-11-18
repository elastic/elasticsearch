/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.scheduler;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.xpack.prelert.job.DataCounts;
import org.elasticsearch.xpack.prelert.job.SchedulerState;
import org.elasticsearch.xpack.prelert.job.audit.Auditor;
import org.elasticsearch.xpack.prelert.job.data.DataProcessor;
import org.elasticsearch.xpack.prelert.job.extraction.DataExtractor;
import org.elasticsearch.xpack.prelert.job.messages.Messages;
import org.elasticsearch.xpack.prelert.job.process.autodetect.params.DataLoadParams;
import org.elasticsearch.xpack.prelert.job.process.autodetect.params.InterimResultsParams;
import org.elasticsearch.xpack.prelert.job.process.autodetect.params.TimeRange;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.function.Supplier;

class ScheduledJob {

    private static final DataLoadParams DATA_LOAD_PARAMS = new DataLoadParams(TimeRange.builder().build());
    private static final int NEXT_TASK_DELAY_MS = 100;

    private final Logger logger;
    private final Auditor auditor;
    private final String jobId;
    private final long frequencyMs;
    private final long queryDelayMs;
    private final DataExtractor dataExtractor;
    private final DataProcessor dataProcessor;
    private final Supplier<Long> currentTimeSupplier;

    private volatile long lookbackStartTimeMs;
    private volatile Long lastEndTimeMs;
    private volatile boolean running = true;

    ScheduledJob(String jobId, long frequencyMs, long queryDelayMs, DataExtractor dataExtractor,
                 DataProcessor dataProcessor, Auditor auditor, Supplier<Long> currentTimeSupplier,
                 long latestFinalBucketEndTimeMs, long latestRecordTimeMs) {
        this.logger = Loggers.getLogger(jobId);
        this.jobId = jobId;
        this.frequencyMs = frequencyMs;
        this.queryDelayMs = queryDelayMs;
        this.dataExtractor = dataExtractor;
        this.dataProcessor = dataProcessor;
        this.auditor = auditor;
        this.currentTimeSupplier = currentTimeSupplier;

        long lastEndTime = Math.max(latestFinalBucketEndTimeMs, latestRecordTimeMs);
        if (lastEndTime > 0) {
            lastEndTimeMs = lastEndTime;
        }
    }

    Long runLookBack(SchedulerState schedulerState) throws Exception {
        long startMs = schedulerState.getStartTimeMillis();
        lookbackStartTimeMs = (lastEndTimeMs != null && lastEndTimeMs + 1 > startMs) ? lastEndTimeMs + 1 : startMs;

        Optional<Long> endMs = Optional.ofNullable(schedulerState.getEndTimeMillis());
        long lookbackEnd = endMs.orElse(currentTimeSupplier.get() - queryDelayMs);
        boolean isLookbackOnly = endMs.isPresent();
        if (lookbackEnd <= lookbackStartTimeMs) {
            if (isLookbackOnly) {
                return null;
            } else {
                auditor.info(Messages.getMessage(Messages.JOB_AUDIT_SCHEDULER_STARTED_REALTIME));
                return nextRealtimeTimestamp();
            }
        }

        String msg = Messages.getMessage(Messages.JOB_AUDIT_SCHEDULER_STARTED_FROM_TO,
                DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.printer().print(lookbackStartTimeMs),
                DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.printer().print(lookbackEnd));
        auditor.info(msg);

        run(lookbackStartTimeMs, lookbackEnd, InterimResultsParams.builder().calcInterim(true).build());
        auditor.info(Messages.getMessage(Messages.JOB_AUDIT_SCHEDULER_LOOKBACK_COMPLETED));
        logger.info("Lookback has finished");
        if (isLookbackOnly) {
            return null;
        } else {
            auditor.info(Messages.getMessage(Messages.JOB_AUDIT_SCHEDULER_CONTINUED_REALTIME));
            return nextRealtimeTimestamp();
        }
    }

    long runRealtime() throws Exception {
        long start = lastEndTimeMs == null ? lookbackStartTimeMs : lastEndTimeMs + 1;
        long nowMinusQueryDelay = currentTimeSupplier.get() - queryDelayMs;
        long end = toIntervalStartEpochMs(nowMinusQueryDelay);
        InterimResultsParams.Builder flushParams = InterimResultsParams.builder()
                .calcInterim(true)
                .advanceTime(String.valueOf(lastEndTimeMs));
        run(start, end, flushParams.build());
        return nextRealtimeTimestamp();
    }

    public void stop() {
        running = false;
        dataExtractor.cancel();
        auditor.info(Messages.getMessage(Messages.JOB_AUDIT_SCHEDULER_STOPPED));
    }

    public boolean isRunning() {
        return running;
    }

    private void run(long start, long end, InterimResultsParams flushParams) throws IOException {
        if (end <= start) {
            return;
        }

        logger.trace("Searching data in: [" + start + ", " + end + ")");

        RuntimeException error = null;
        long recordCount = 0;
        dataExtractor.newSearch(start, end, logger);
        while (running && dataExtractor.hasNext()) {
            Optional<InputStream> extractedData;
            try {
                extractedData = dataExtractor.next();
            } catch (Exception e) {
                error = new ExtractionProblemException(e);
                break;
            }
            if (extractedData.isPresent()) {
                DataCounts counts;
                try {
                    counts = dataProcessor.processData(jobId, extractedData.get(), DATA_LOAD_PARAMS);
                } catch (Exception e) {
                    error = new AnalysisProblemException(e);
                    break;
                }
                recordCount += counts.getProcessedRecordCount();
                if (counts.getLatestRecordTimeStamp() != null) {
                    lastEndTimeMs = counts.getLatestRecordTimeStamp().getTime();
                }
            }
        }

        lastEndTimeMs = Math.max(lastEndTimeMs == null ? 0 : lastEndTimeMs, end - 1);

        // Ensure time is always advanced in order to avoid importing duplicate data.
        // This is the reason we store the error rather than throw inline.
        if (error != null) {
            throw error;
        }

        if (recordCount == 0) {
            throw new EmptyDataCountException();
        }

        dataProcessor.flushJob(jobId, flushParams);
    }

    private long nextRealtimeTimestamp() {
        long epochMs = currentTimeSupplier.get() + frequencyMs;
        return toIntervalStartEpochMs(epochMs) + NEXT_TASK_DELAY_MS;
    }

    private long toIntervalStartEpochMs(long epochMs) {
        return (epochMs / frequencyMs) * frequencyMs;
    }

    class AnalysisProblemException extends RuntimeException {

        final long nextDelayInMsSinceEpoch = nextRealtimeTimestamp();

        AnalysisProblemException(Throwable cause) {
            super(cause);
        }

    }

    class ExtractionProblemException extends RuntimeException {

        final long nextDelayInMsSinceEpoch = nextRealtimeTimestamp();

        ExtractionProblemException(Throwable cause) {
            super(cause);
        }
    }

    class EmptyDataCountException extends RuntimeException {

        final long nextDelayInMsSinceEpoch = nextRealtimeTimestamp();

        EmptyDataCountException() {}
    }

}
