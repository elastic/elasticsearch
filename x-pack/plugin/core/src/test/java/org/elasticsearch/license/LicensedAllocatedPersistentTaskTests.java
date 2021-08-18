/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.function.Consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class LicensedAllocatedPersistentTaskTests extends ESTestCase {

    void assertTrackingComplete(Consumer<LicensedAllocatedPersistentTask> method) {
        XPackLicenseState licenseState = mock(XPackLicenseState.class);
        LicensedFeature.Persistent feature = LicensedFeature.persistent("somefeature", License.OperationMode.PLATINUM);
        var task = new LicensedAllocatedPersistentTask(0, "type", "action", "description", TaskId.EMPTY_TASK_ID, Map.of(),
            feature, "context", licenseState);
        PersistentTasksService service = mock(PersistentTasksService.class);
        TaskManager taskManager = mock(TaskManager.class);
        task.init(service, taskManager, "id", 0);
        verify(licenseState, times(1)).enableUsageTracking(feature, "context");
        method.accept(task);
        verify(licenseState, times(1)).disableUsageTracking(feature, "context");
    }

    public void testCompleted() {
        assertTrackingComplete(LicensedAllocatedPersistentTask::markAsCompleted);
    }

    public void testCancelled() {
        assertTrackingComplete(LicensedAllocatedPersistentTask::markAsCancelled);
    }

    public void testFailed() {
        assertTrackingComplete(t -> t.markAsFailed(null));
    }

    public void testLocallyAborted() {
        assertTrackingComplete(t -> t.markAsLocallyAborted("reason"));
    }
}
