/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.xpack.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.ml.job.config.ModelPlotConfig;

import java.util.List;

public final class UpdateParams {

    private final ModelPlotConfig modelPlotConfig;
    private final List<JobUpdate.DetectorUpdate> detectorUpdates;
    private final boolean updateScheduledEvents;

    public UpdateParams(@Nullable ModelPlotConfig modelPlotConfig,
                        @Nullable List<JobUpdate.DetectorUpdate> detectorUpdates,
                        boolean updateScheduledEvents) {
        this.modelPlotConfig = modelPlotConfig;
        this.detectorUpdates = detectorUpdates;
        this.updateScheduledEvents = updateScheduledEvents;
    }

    public ModelPlotConfig getModelPlotConfig() {
        return modelPlotConfig;
    }

    public List<JobUpdate.DetectorUpdate> getDetectorUpdates() {
        return detectorUpdates;
    }

    public boolean isUpdateScheduledEvents() {
        return updateScheduledEvents;
    }
}
