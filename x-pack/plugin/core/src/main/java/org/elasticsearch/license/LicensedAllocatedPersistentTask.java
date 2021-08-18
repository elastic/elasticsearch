/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;

import java.util.Map;

/**
 * An AllocatedPersistentTask which automatically tracks as a licensed feature usage.
 */
public class LicensedAllocatedPersistentTask extends AllocatedPersistentTask {
    private final LicensedFeature.Persistent licensedFeature;
    private final String featureContext;
    private final XPackLicenseState licenseState;

    public LicensedAllocatedPersistentTask(long id, String type, String action, String description, TaskId parentTask,
                                           Map<String, String> headers, LicensedFeature.Persistent feature, String featureContext,
                                           XPackLicenseState licenseState) {
        super(id, type, action, description, parentTask, headers);
        this.licensedFeature = feature;
        this.featureContext = featureContext;
        this.licenseState = licenseState;
        licensedFeature.startTracking(licenseState, featureContext);
    }

    private void stopTracking() {
        licensedFeature.stopTracking(licenseState, featureContext);
    }

    @Override
    protected boolean markAsCancelled() {
        stopTracking();
        return super.markAsCancelled();
    }

    @Override
    public void markAsCompleted() {
        stopTracking();
        super.markAsCompleted();
    }

    @Override
    public void markAsFailed(Exception e) {
        stopTracking();
        super.markAsFailed(e);
    }

    @Override
    public void markAsLocallyAborted(String localAbortReason) {
        stopTracking();
        super.markAsLocallyAborted(localAbortReason);
    }

    // this is only overridden so that tests can run it
    @Override
    protected void init(PersistentTasksService persistentTasksService, TaskManager taskManager,
                        String persistentTaskId, long allocationId) {
        super.init(persistentTasksService, taskManager, persistentTaskId, allocationId);
    }
}
