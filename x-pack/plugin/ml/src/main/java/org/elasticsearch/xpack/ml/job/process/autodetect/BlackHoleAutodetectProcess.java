/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;
import org.elasticsearch.xpack.core.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.config.ModelPlotConfig;
import org.elasticsearch.xpack.core.ml.job.config.PerPartitionCategorizationConfig;
import org.elasticsearch.xpack.ml.job.persistence.StateStreamer;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.output.FlushAcknowledgement;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.DataLoadParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.FlushJobParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.ForecastParams;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.ml.job.results.AutodetectResult;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * A placeholder class simulating the actions of the native Autodetect process.
 * Most methods consume data without performing any action however, after a call to
 * {@link #flushJob(FlushJobParams)} a {@link FlushAcknowledgement}
 * message is expected on the {@link #readAutodetectResults()} ()} stream. This class writes the flush
 * acknowledgement immediately.
 */
public class BlackHoleAutodetectProcess implements AutodetectProcess {

    public static final String MAGIC_FAILURE_VALUE = "253402300799";
    public static final String MAGIC_FAILURE_VALUE_AS_DATE = "9999-12-31 23:59:59";

    private static final String FLUSH_ID = "flush-1";

    private final String jobId;
    private final ZonedDateTime startTime;
    private final BlockingQueue<AutodetectResult> results = new LinkedBlockingDeque<>();
    private final Consumer<String> onProcessCrash;
    private volatile boolean open = true;

    public BlackHoleAutodetectProcess(String jobId, Consumer<String> onProcessCrash) {
        this.jobId = jobId;
        startTime = ZonedDateTime.now();
        this.onProcessCrash = Objects.requireNonNull(onProcessCrash);
    }

    @Override
    public void restoreState(StateStreamer stateStreamer, ModelSnapshot modelSnapshot) {
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public void writeRecord(String[] record) {
        if (Arrays.asList(record).contains(MAGIC_FAILURE_VALUE)) {
            open = false;
            onProcessCrash.accept("simulated failure");
            AutodetectResult result = new AutodetectResult(null, null, null, null, null, null, null, null, null, null, null, null, null);
            results.add(result);
        }
    }

    @Override
    public void writeResetBucketsControlMessage(DataLoadParams params) {
    }

    @Override
    public void writeUpdateModelPlotMessage(ModelPlotConfig modelPlotConfig) {
    }

    @Override
    public void writeUpdatePerPartitionCategorizationMessage(PerPartitionCategorizationConfig perPartitionCategorizationConfig) {
    }

    @Override
    public void writeUpdateDetectorRulesMessage(int detectorIndex, List<DetectionRule> rules) {
    }

    @Override
    public void writeUpdateFiltersMessage(List<MlFilter> filters) {
    }

    @Override
    public void writeUpdateScheduledEventsMessage(List<ScheduledEvent> events, TimeValue bucketSpan) {
    }

    /**
     * Accept the request do nothing with it but write the flush acknowledgement to {@link #readAutodetectResults()}
     * @param params Should interim results be generated
     * @return {@link #FLUSH_ID}
     */
    @Override
    public String flushJob(FlushJobParams params) {
        FlushAcknowledgement flushAcknowledgement = new FlushAcknowledgement(FLUSH_ID, 0L);
        AutodetectResult result =
            new AutodetectResult(null, null, null, null, null, null, null, null, null, null, null, null, flushAcknowledgement);
        results.add(result);
        return FLUSH_ID;
    }

    @Override
    public void persistState() {
    }

    @Override
    public void flushStream() {
    }

    @Override
    public void close() {
        if (open) {
            Quantiles quantiles = new Quantiles(jobId, new Date(), "black hole quantiles");
            AutodetectResult result =
                new AutodetectResult(null, null, null, quantiles, null, null, null, null, null, null, null, null, null);
            results.add(result);
            open = false;
        }
    }

    @Override
    public void kill() {
        open = false;
    }

    @Override
    public Iterator<AutodetectResult> readAutodetectResults() {
        // Create a custom iterator here, because LinkedBlockingDeque iterator and stream are not blocking when empty:
        return new Iterator<AutodetectResult>() {

            AutodetectResult result;

            @Override
            public boolean hasNext() {
                try {
                    while (open) {
                        result = results.poll(100, TimeUnit.MILLISECONDS);
                        if (result != null) {
                            return true;
                        }
                    }
                    result = results.poll();
                    return result != null;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }

            @Override
            public AutodetectResult next() {
                return result;
            }
        };
    }

    @Override
    public void consumeAndCloseOutputStream() {
    }

    @Override
    public ZonedDateTime getProcessStartTime() {
        return startTime;
    }

    @Override
    public boolean isProcessAlive() {
        return open;
    }

    @Override
    public boolean isProcessAliveAfterWaiting() {
        try {
            Thread.sleep(45);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return open;
    }

    @Override
    public String readError() {
        return "";
    }

    @Override
    public void forecastJob(ForecastParams params) {
    }
}
