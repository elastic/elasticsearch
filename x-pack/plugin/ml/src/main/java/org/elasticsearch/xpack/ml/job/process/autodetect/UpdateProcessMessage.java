/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.config.ModelPlotConfig;
import org.elasticsearch.xpack.core.ml.job.config.PerPartitionCategorizationConfig;

import java.util.List;

public final class UpdateProcessMessage {

    @Nullable private final ModelPlotConfig modelPlotConfig;
    @Nullable private final PerPartitionCategorizationConfig perPartitionCategorizationConfig;
    @Nullable private final List<JobUpdate.DetectorUpdate> detectorUpdates;
    @Nullable private final List<MlFilter> filters;
    @Nullable private final List<ScheduledEvent> scheduledEvents;

    private UpdateProcessMessage(@Nullable ModelPlotConfig modelPlotConfig,
                                 @Nullable PerPartitionCategorizationConfig perPartitionCategorizationConfig,
                                 @Nullable List<JobUpdate.DetectorUpdate> detectorUpdates,
                                 @Nullable List<MlFilter> filters, List<ScheduledEvent> scheduledEvents) {
        this.modelPlotConfig = modelPlotConfig;
        this.perPartitionCategorizationConfig = perPartitionCategorizationConfig;
        this.detectorUpdates = detectorUpdates;
        this.filters = filters;
        this.scheduledEvents = scheduledEvents;
    }

    @Nullable
    public ModelPlotConfig getModelPlotConfig() {
        return modelPlotConfig;
    }

    @Nullable
    public PerPartitionCategorizationConfig getPerPartitionCategorizationConfig() {
        return perPartitionCategorizationConfig;
    }

    @Nullable
    public List<JobUpdate.DetectorUpdate> getDetectorUpdates() {
        return detectorUpdates;
    }

    @Nullable
    public List<MlFilter> getFilters() {
        return filters;
    }

    @Nullable
    public List<ScheduledEvent> getScheduledEvents() {
        return scheduledEvents;
    }

    public static class Builder {

        @Nullable private ModelPlotConfig modelPlotConfig;
        @Nullable private PerPartitionCategorizationConfig perPartitionCategorizationConfig;
        @Nullable private List<JobUpdate.DetectorUpdate> detectorUpdates;
        @Nullable private List<MlFilter> filters;
        @Nullable private List<ScheduledEvent> scheduledEvents;

        public Builder setModelPlotConfig(ModelPlotConfig modelPlotConfig) {
            this.modelPlotConfig = modelPlotConfig;
            return this;
        }

        public Builder setPerPartitionCategorizationConfig(PerPartitionCategorizationConfig perPartitionCategorizationConfig) {
            this.perPartitionCategorizationConfig = perPartitionCategorizationConfig;
            return this;
        }

        public Builder setDetectorUpdates(List<JobUpdate.DetectorUpdate> detectorUpdates) {
            this.detectorUpdates = detectorUpdates;
            return this;
        }

        public Builder setFilters(List<MlFilter> filters) {
            this.filters = filters;
            return this;
        }

        public Builder setScheduledEvents(List<ScheduledEvent> scheduledEvents) {
            this.scheduledEvents = scheduledEvents;
            return this;
        }

        public UpdateProcessMessage build() {
            return new UpdateProcessMessage(modelPlotConfig, perPartitionCategorizationConfig, detectorUpdates, filters, scheduledEvents);
        }
    }
}
