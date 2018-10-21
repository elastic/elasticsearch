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
import org.elasticsearch.xpack.ml.job.persistence.StateStreamer;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.DataLoadParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.FlushJobParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.ForecastParams;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.job.results.AutodetectResult;

import java.io.Closeable;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.List;

/**
 * Interface representing the native C++ autodetect process
 */
public interface AutodetectProcess extends Closeable {

    /**
     * Restore state from the given {@link ModelSnapshot}
     * @param stateStreamer the streamer of the job state
     * @param modelSnapshot the model snapshot to restore
     */
    void restoreState(StateStreamer stateStreamer, ModelSnapshot modelSnapshot);

    /**
     * Is the process ready to receive data?
     * @return {@code true} if the process is ready to receive data
     */
    boolean isReady();

    /**
     * Write the record to autodetect. The record parameter should not be encoded
     * (i.e. length encoded) the implementation will appy the corrrect encoding.
     *
     * @param record Plain array of strings, implementors of this class should
     *               encode the record appropriately
     * @throws IOException If the write failed
     */
    void writeRecord(String[] record) throws IOException;

    /**
     * Write the reset buckets control message
     *
     * @param params Reset bucket options
     * @throws IOException If write reset message fails
     */
    void writeResetBucketsControlMessage(DataLoadParams params) throws IOException;

    /**
     * Update the model plot configuration
     *
     * @param modelPlotConfig New model plot config
     * @throws IOException If the write fails
     */
    void writeUpdateModelPlotMessage(ModelPlotConfig modelPlotConfig) throws IOException;

    /**
     * Write message to update the detector rules
     *
     * @param detectorIndex Index of the detector to update
     * @param rules Detector rules
     * @throws IOException If the write fails
     */
    void writeUpdateDetectorRulesMessage(int detectorIndex, List<DetectionRule> rules)
            throws IOException;

    /**
     * Write message to update the filters
     *
     * @param filters the filters to update
     * @throws IOException If the write fails
     */
    void writeUpdateFiltersMessage(List<MlFilter> filters) throws IOException;

    /**
     * Write message to update the scheduled events
     *
     * @param events Scheduled events
     * @param bucketSpan The job bucket span
     * @throws IOException If the write fails
     */
    void writeUpdateScheduledEventsMessage(List<ScheduledEvent> events, TimeValue bucketSpan) throws IOException;

    /**
     * Flush the job pushing any stale data into autodetect.
     * Every flush command generates a unique flush Id which will be output
     * in a flush acknowledgment by the autodetect process once the flush has
     * been processed.
     *
     * @param params Parameters describing the controls that will accompany the flushing
     *               (e.g. calculating interim results, time control, etc.)
     * @return The flush Id
     * @throws IOException If the flush failed
     */
    String flushJob(FlushJobParams params) throws IOException;

    /**
     * Do a forecast on a running job.
     *
     * @param params The forecast parameters
     * @throws IOException If the write fails
     */
    void forecastJob(ForecastParams params) throws IOException;

    /**
     * Ask the job to start persisting model state in the background
     * @throws IOException If writing the request fails
     */
    void persistJob() throws IOException;

    /**
     * Flush the output data stream
     */
    void flushStream() throws IOException;

    /**
     * Kill the process.  Do not wait for it to stop gracefully.
     */
    void kill() throws IOException;

    /**
     * @return stream of autodetect results.
     */
    Iterator<AutodetectResult> readAutodetectResults();

    /**
     * The time the process was started
     * @return Process start time
     */
    ZonedDateTime getProcessStartTime();

    /**
     * Returns true if the process still running.
     * Methods such as {@link #flushJob(FlushJobParams)} are essentially
     * asynchronous the command will be continue to execute in the process after
     * the call has returned. This method tests whether something catastrophic
     * occurred in the process during its execution.
     * @return True if the process is still running
     */
    boolean isProcessAlive();

    /**
     * Check whether autodetect terminated given maximum 45ms for termination
     *
     * Processing errors are highly likely caused by autodetect being unexpectedly
     * terminated.
     *
     * Workaround: As we can not easily check if autodetect is alive, we rely on
     * the logPipe being ended. As the loghandler runs in another thread which
     * might fall behind this one, we give it a grace period of 45ms.
     *
     * @return false if process has ended for sure, true if it probably still runs
     */
    boolean isProcessAliveAfterWaiting();

    /**
     * Read any content in the error output buffer.
     * @return An error message or empty String if no error.
     */
    String readError();
}
