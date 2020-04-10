/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.params;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.TimingStats;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class AutodetectParams {

    private final DataCounts dataCounts;
    private final ModelSizeStats modelSizeStats;
    @Nullable
    private final TimingStats timingStats;
    @Nullable
    private final ModelSnapshot modelSnapshot;
    @Nullable
    private final Quantiles quantiles;
    private final Set<MlFilter> filters;
    private final List<ScheduledEvent> scheduledEvents;


    private AutodetectParams(DataCounts dataCounts, ModelSizeStats modelSizeStats, TimingStats timingStats,
                             @Nullable ModelSnapshot modelSnapshot,
                             @Nullable Quantiles quantiles, Set<MlFilter> filters,
                             List<ScheduledEvent> scheduledEvents) {
        this.dataCounts = Objects.requireNonNull(dataCounts);
        this.modelSizeStats = Objects.requireNonNull(modelSizeStats);
        this.timingStats = timingStats;
        this.modelSnapshot = modelSnapshot;
        this.quantiles = quantiles;
        this.filters = filters;
        this.scheduledEvents = scheduledEvents;
    }

    public DataCounts dataCounts() {
        return dataCounts;
    }

    public ModelSizeStats modelSizeStats() {
        return modelSizeStats;
    }

    public TimingStats timingStats() {
        return timingStats;
    }

    @Nullable
    public ModelSnapshot modelSnapshot() {
        return modelSnapshot;
    }

    @Nullable
    public Quantiles quantiles() {
        return quantiles;
    }

    public Set<MlFilter> filters() {
        return filters;
    }

    public List<ScheduledEvent> scheduledEvents() {
        return scheduledEvents;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other instanceof AutodetectParams == false) {
            return false;
        }

        AutodetectParams that = (AutodetectParams) other;

        return Objects.equals(this.dataCounts, that.dataCounts)
                && Objects.equals(this.modelSizeStats, that.modelSizeStats)
                && Objects.equals(this.timingStats, that.timingStats)
                && Objects.equals(this.modelSnapshot, that.modelSnapshot)
                && Objects.equals(this.quantiles, that.quantiles)
                && Objects.equals(this.filters, that.filters)
                && Objects.equals(this.scheduledEvents, that.scheduledEvents);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataCounts, modelSizeStats, timingStats, modelSnapshot, quantiles, filters, scheduledEvents);
    }

    public static class Builder {

        private DataCounts dataCounts;
        private ModelSizeStats modelSizeStats;
        private TimingStats timingStats;
        private ModelSnapshot modelSnapshot;
        private Quantiles quantiles;
        private Set<MlFilter> filters;
        private List<ScheduledEvent> scheduledEvents;

        public Builder(String jobId) {
            dataCounts = new DataCounts(jobId);
            modelSizeStats = new ModelSizeStats.Builder(jobId).build();
            timingStats = new TimingStats(jobId);
            filters = new HashSet<>();
            scheduledEvents = new ArrayList<>();
        }

        public Builder setDataCounts(DataCounts dataCounts) {
            this.dataCounts = dataCounts;
            return this;
        }

        public DataCounts getDataCounts() {
            return dataCounts;
        }

        public Builder setModelSizeStats(ModelSizeStats modelSizeStats) {
            this.modelSizeStats = modelSizeStats;
            return this;
        }

        public Builder setTimingStats(TimingStats timingStats) {
            this.timingStats = new TimingStats(timingStats);
            return this;
        }

        public Builder setModelSnapshot(ModelSnapshot modelSnapshot) {
            this.modelSnapshot = modelSnapshot;
            return this;
        }

        public Builder setQuantiles(Quantiles quantiles) {
            this.quantiles = quantiles;
            return this;
        }

        public Builder setScheduledEvents(List<ScheduledEvent> scheduledEvents) {
            this.scheduledEvents = scheduledEvents;
            return this;
        }

        public Builder addFilter(MlFilter filter) {
            filters.add(filter);
            return this;
        }

        public Builder setFilters(Set<MlFilter> filters) {
            filters = Objects.requireNonNull(filters);
            return this;
        }

        public AutodetectParams build() {
            return new AutodetectParams(dataCounts, modelSizeStats, timingStats, modelSnapshot, quantiles,
                    filters, scheduledEvents);
        }
    }
}
