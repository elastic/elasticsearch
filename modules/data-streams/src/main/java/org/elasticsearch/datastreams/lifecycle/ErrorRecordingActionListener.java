/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.ProjectId;

import static org.apache.logging.log4j.LogManager.getLogger;

/**
 * Action listener that records the encountered failure using the provided recordError callback for the
 * provided target index. If the listener is notified of success it will clear the recorded entry for the provided
 * target index using the clearErrorRecord callback.
 */
public class ErrorRecordingActionListener implements ActionListener<Void> {

    private static final Logger logger = getLogger(ErrorRecordingActionListener.class);

    private final String actionName;
    private final ProjectId projectId;
    private final String targetIndex;
    private final DataStreamLifecycleErrorStore errorStore;
    private final String errorLogMessage;
    private final int signallingErrorRetryThreshold;

    public ErrorRecordingActionListener(
        String actionName,
        ProjectId projectId,
        String targetIndex,
        DataStreamLifecycleErrorStore errorStore,
        String errorLogMessage,
        int signallingErrorRetryThreshold
    ) {
        this.actionName = actionName;
        this.projectId = projectId;
        this.targetIndex = targetIndex;
        this.errorStore = errorStore;
        this.errorLogMessage = errorLogMessage;
        this.signallingErrorRetryThreshold = signallingErrorRetryThreshold;
    }

    @Override
    public void onResponse(Void unused) {
        logger.trace("Clearing recorded error for index [{}] because the [{}] action was successful", targetIndex, actionName);
        errorStore.clearRecordedError(projectId, targetIndex);
    }

    @Override
    public void onFailure(Exception e) {
        errorStore.recordAndLogError(projectId, targetIndex, e, errorLogMessage, signallingErrorRetryThreshold);
    }

}
