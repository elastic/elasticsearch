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
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.job.persistence.StateStreamer;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.DataLoadParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.FlushJobParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.ForecastParams;
import org.elasticsearch.xpack.ml.job.results.AutodetectResult;
import org.elasticsearch.xpack.ml.process.NativeProcess;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Interface representing the native C++ autodetect process
 */
public interface AutodetectProcess extends NativeProcess {

    /**
     * Restore state from the given {@link ModelSnapshot}
     * @param stateStreamer the streamer of the job state
     * @param modelSnapshot the model snapshot to restore
     */
    void restoreState(StateStreamer stateStreamer, ModelSnapshot modelSnapshot);

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
     * Update the per-partition categorization configuration
     *
     * @param perPartitionCategorizationConfig New per-partition categorization config
     * @throws IOException If the write fails
     */
    void writeUpdatePerPartitionCategorizationMessage(PerPartitionCategorizationConfig perPartitionCategorizationConfig) throws IOException;

    /**
     * Write message to update the detector rules
     *
     * @param detectorIndex Index of the detector to update
     * @param rules Detector rules
     * @throws IOException If the write fails
     */
    void writeUpdateDetectorRulesMessage(int detectorIndex, List<DetectionRule> rules) throws IOException;

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
     * @return stream of autodetect results.
     */
    Iterator<AutodetectResult> readAutodetectResults();

    /**
     * Read anything left in the stream before
     * closing the stream otherwise if the process
     * tries to write more after the close it gets
     * a SIGPIPE
     */
    void consumeAndCloseOutputStream();
}
