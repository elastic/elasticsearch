/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.config.ModelPlotConfig;
import org.elasticsearch.xpack.core.ml.job.config.PerPartitionCategorizationConfig;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public final class UpdateParams {

    private final String jobId;
    private final ModelPlotConfig modelPlotConfig;
    private final PerPartitionCategorizationConfig perPartitionCategorizationConfig;
    private final List<JobUpdate.DetectorUpdate> detectorUpdates;
    private final MlFilter filter;
    private final boolean updateScheduledEvents;

    private UpdateParams(
        String jobId,
        @Nullable ModelPlotConfig modelPlotConfig,
        @Nullable PerPartitionCategorizationConfig perPartitionCategorizationConfig,
        @Nullable List<JobUpdate.DetectorUpdate> detectorUpdates,
        @Nullable MlFilter filter,
        boolean updateScheduledEvents
    ) {
        this.jobId = Objects.requireNonNull(jobId);
        this.modelPlotConfig = modelPlotConfig;
        this.perPartitionCategorizationConfig = perPartitionCategorizationConfig;
        this.detectorUpdates = detectorUpdates;
        this.filter = filter;
        this.updateScheduledEvents = updateScheduledEvents;
    }

    public String getJobId() {
        return jobId;
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
    public MlFilter getFilter() {
        return filter;
    }

    /**
     * Returns true if the update params include a job update,
     * ie an update to the job config directly rather than an
     * update to external resources a job uses (e.g. calendars, filters).
     */
    public boolean isJobUpdate() {
        return modelPlotConfig != null || detectorUpdates != null || perPartitionCategorizationConfig != null;
    }

    public boolean isUpdateScheduledEvents() {
        return updateScheduledEvents;
    }

    /**
     * Returns all filters referenced by this update
     * @return all referenced filters
     */
    public Set<String> extractReferencedFilters() {
        Set<String> filterIds = new HashSet<>();
        if (filter != null) {
            filterIds.add(filter.getId());
        }
        if (detectorUpdates != null) {
            detectorUpdates.forEach(
                detectorUpdate -> detectorUpdate.getRules().forEach(rule -> filterIds.addAll(rule.extractReferencedFilters()))
            );
        }
        return filterIds;
    }

    public static UpdateParams fromJobUpdate(JobUpdate jobUpdate) {
        return new Builder(jobUpdate.getJobId()).modelPlotConfig(jobUpdate.getModelPlotConfig())
            .perPartitionCategorizationConfig(jobUpdate.getPerPartitionCategorizationConfig())
            .detectorUpdates(jobUpdate.getDetectorUpdates())
            .updateScheduledEvents(jobUpdate.getGroups() != null)
            .build();
    }

    public static UpdateParams filterUpdate(String jobId, MlFilter filter) {
        return new Builder(jobId).filter(filter).build();
    }

    public static UpdateParams scheduledEventsUpdate(String jobId) {
        return new Builder(jobId).updateScheduledEvents(true).build();
    }

    public static Builder builder(String jobId) {
        return new Builder(jobId);
    }

    public static class Builder {

        private String jobId;
        private ModelPlotConfig modelPlotConfig;
        private PerPartitionCategorizationConfig perPartitionCategorizationConfig;
        private List<JobUpdate.DetectorUpdate> detectorUpdates;
        private MlFilter filter;
        private boolean updateScheduledEvents;

        public Builder(String jobId) {
            this.jobId = Objects.requireNonNull(jobId);
        }

        public Builder modelPlotConfig(ModelPlotConfig modelPlotConfig) {
            this.modelPlotConfig = modelPlotConfig;
            return this;
        }

        public Builder perPartitionCategorizationConfig(PerPartitionCategorizationConfig perPartitionCategorizationConfig) {
            this.perPartitionCategorizationConfig = perPartitionCategorizationConfig;
            return this;
        }

        public Builder detectorUpdates(List<JobUpdate.DetectorUpdate> detectorUpdates) {
            this.detectorUpdates = detectorUpdates;
            return this;
        }

        public Builder filter(MlFilter filter) {
            this.filter = filter;
            return this;
        }

        public Builder updateScheduledEvents(boolean updateScheduledEvents) {
            this.updateScheduledEvents = updateScheduledEvents;
            return this;
        }

        public UpdateParams build() {
            return new UpdateParams(
                jobId,
                modelPlotConfig,
                perPartitionCategorizationConfig,
                detectorUpdates,
                filter,
                updateScheduledEvents
            );
        }
    }
}
