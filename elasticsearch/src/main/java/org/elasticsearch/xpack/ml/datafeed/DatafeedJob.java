/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.xpack.ml.action.FlushJobAction;
import org.elasticsearch.xpack.ml.action.PostDataAction;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.ml.job.config.DataDescription;
import org.elasticsearch.xpack.ml.notifications.Auditor;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

class DatafeedJob {

    private static final Logger LOGGER = Loggers.getLogger(DatafeedJob.class);
    private static final int NEXT_TASK_DELAY_MS = 100;

    private final Auditor auditor;
    private final String jobId;
    private final DataDescription dataDescription;
    private final long frequencyMs;
    private final long queryDelayMs;
    private final Client client;
    private final DataExtractorFactory dataExtractorFactory;
    private final Supplier<Long> currentTimeSupplier;

    private volatile long lookbackStartTimeMs;
    private volatile Long lastEndTimeMs;
    private AtomicBoolean running = new AtomicBoolean(true);

    DatafeedJob(String jobId, DataDescription dataDescription, long frequencyMs, long queryDelayMs,
                 DataExtractorFactory dataExtractorFactory, Client client, Auditor auditor, Supplier<Long> currentTimeSupplier,
                 long latestFinalBucketEndTimeMs, long latestRecordTimeMs) {
        this.jobId = jobId;
        this.dataDescription = Objects.requireNonNull(dataDescription);
        this.frequencyMs = frequencyMs;
        this.queryDelayMs = queryDelayMs;
        this.dataExtractorFactory = dataExtractorFactory;
        this.client = client;
        this.auditor = auditor;
        this.currentTimeSupplier = currentTimeSupplier;

        long lastEndTime = Math.max(latestFinalBucketEndTimeMs, latestRecordTimeMs);
        if (lastEndTime > 0) {
            lastEndTimeMs = lastEndTime;
        }
    }

    Long runLookBack(long startTime, Long endTime) throws Exception {
        lookbackStartTimeMs = (lastEndTimeMs != null && lastEndTimeMs + 1 > startTime) ? lastEndTimeMs + 1 : startTime;
        Optional<Long> endMs = Optional.ofNullable(endTime);
        long lookbackEnd = endMs.orElse(currentTimeSupplier.get() - queryDelayMs);
        boolean isLookbackOnly = endMs.isPresent();
        if (lookbackEnd <= lookbackStartTimeMs) {
            if (isLookbackOnly) {
                return null;
            } else {
                auditor.info(Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_STARTED_REALTIME));
                return nextRealtimeTimestamp();
            }
        }

        String msg = Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_STARTED_FROM_TO,
                DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.printer().print(lookbackStartTimeMs),
                DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.printer().print(lookbackEnd));
        auditor.info(msg);

        FlushJobAction.Request request = new FlushJobAction.Request(jobId);
        request.setCalcInterim(true);
        run(lookbackStartTimeMs, lookbackEnd, request);
        auditor.info(Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_LOOKBACK_COMPLETED));
        LOGGER.info("[{}] Lookback has finished", jobId);
        if (isLookbackOnly) {
            return null;
        } else {
            auditor.info(Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_CONTINUED_REALTIME));
            return nextRealtimeTimestamp();
        }
    }

    long runRealtime() throws Exception {
        long start = lastEndTimeMs == null ? lookbackStartTimeMs : lastEndTimeMs + 1;
        long nowMinusQueryDelay = currentTimeSupplier.get() - queryDelayMs;
        long end = toIntervalStartEpochMs(nowMinusQueryDelay);
        FlushJobAction.Request request = new FlushJobAction.Request(jobId);
        request.setCalcInterim(true);
        request.setAdvanceTime(String.valueOf(lastEndTimeMs));
        run(start, end, request);
        return nextRealtimeTimestamp();
    }

    /**
     * Stops the datafeed job
     *
     * @return <code>true</code> when the datafeed was running and this method invocation stopped it,
     *         otherwise <code>false</code> is returned
     */
    public boolean stop() {
        if (running.compareAndSet(true, false)) {
            auditor.info(Messages.getMessage(Messages.JOB_AUDIT_DATAFEED_STOPPED));
            return true;
        } else {
            return false;
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    private void run(long start, long end, FlushJobAction.Request flushRequest) throws IOException {
        if (end <= start) {
            return;
        }

        LOGGER.trace("[{}] Searching data in: [{}, {})", jobId, start, end);

        RuntimeException error = null;
        long recordCount = 0;
        DataExtractor dataExtractor = dataExtractorFactory.newExtractor(start, end);
        while (dataExtractor.hasNext()) {
            if (!isRunning() && !dataExtractor.isCancelled()) {
                dataExtractor.cancel();
            }

            Optional<InputStream> extractedData;
            try {
                extractedData = dataExtractor.next();
            } catch (Exception e) {
                error = new ExtractionProblemException(e);
                break;
            }
            if (extractedData.isPresent()) {
                DataCounts counts;
                try (InputStream in = extractedData.get()) {
                    counts = postData(in);
                } catch (Exception e) {
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
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

        try {
            client.execute(FlushJobAction.INSTANCE, flushRequest).get();
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException(e);
        }
    }

    private DataCounts postData(InputStream inputStream) throws IOException, ExecutionException, InterruptedException {
        PostDataAction.Request request = new PostDataAction.Request(jobId);
        request.setDataDescription(dataDescription);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Streams.copy(inputStream, outputStream);
        request.setContent(new BytesArray(outputStream.toByteArray()));
        PostDataAction.Response response = client.execute(PostDataAction.INSTANCE, request).get();
        return response.getDataCounts();
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
