/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.packageloader.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.tasks.RemovedTaskListener;
import org.elasticsearch.tasks.Task;

public record DownloadTaskRemovedListener(ModelDownloadTask trackedTask, ActionListener<AcknowledgedResponse> listener)
    implements
        RemovedTaskListener {

    @Override
    public void onRemoved(Task task) {
        if (task.getId() == trackedTask.getId()) {
            if (trackedTask.getTaskException() == null) {
                listener.onResponse(AcknowledgedResponse.TRUE);
            } else {
                listener.onFailure(trackedTask.getTaskException());
            }
        }
    }
}
