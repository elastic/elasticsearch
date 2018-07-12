/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.config.ModelPlotConfig;

import java.util.List;

public final class UpdateProcessMessage {

    @Nullable private final ModelPlotConfig modelPlotConfig;
    @Nullable private final List<JobUpdate.DetectorUpdate> detectorUpdates;
    @Nullable private final MlFilter filter;
    @Nullable private final List<ScheduledEvent> scheduledEvents;

    private UpdateProcessMessage(@Nullable ModelPlotConfig modelPlotConfig, @Nullable List<JobUpdate.DetectorUpdate> detectorUpdates,
                                 @Nullable MlFilter filter, List<ScheduledEvent> scheduledEvents) {
        this.modelPlotConfig = modelPlotConfig;
        this.detectorUpdates = detectorUpdates;
        this.filter = filter;
        this.scheduledEvents = scheduledEvents;
    }

    @Nullable
    public ModelPlotConfig getModelPlotConfig() {
        return modelPlotConfig;
    }

    @Nullable
    public List<JobUpdate.DetectorUpdate> getDetectorUpdates() {
        return detectorUpdates;
    }

    @Nullable
    public MlFilter getFilter() {
        return filter;
    }

    @Nullable
    public List<ScheduledEvent> getScheduledEvents() {
        return scheduledEvents;
    }

    public static class Builder {

        @Nullable private ModelPlotConfig modelPlotConfig;
        @Nullable private List<JobUpdate.DetectorUpdate> detectorUpdates;
        @Nullable private MlFilter filter;
        @Nullable private List<ScheduledEvent> scheduledEvents;

        public Builder setModelPlotConfig(ModelPlotConfig modelPlotConfig) {
            this.modelPlotConfig = modelPlotConfig;
            return this;
        }

        public Builder setDetectorUpdates(List<JobUpdate.DetectorUpdate> detectorUpdates) {
            this.detectorUpdates = detectorUpdates;
            return this;
        }

        public Builder setFilter(MlFilter filter) {
            this.filter = filter;
            return this;
        }

        public Builder setScheduledEvents(List<ScheduledEvent> scheduledEvents) {
            this.scheduledEvents = scheduledEvents;
            return this;
        }

        public UpdateProcessMessage build() {
            return new UpdateProcessMessage(modelPlotConfig, detectorUpdates, filter, scheduledEvents);
        }
    }
}
