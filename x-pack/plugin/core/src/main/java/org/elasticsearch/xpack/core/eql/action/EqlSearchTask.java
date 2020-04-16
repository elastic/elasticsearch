/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.eql.action;

import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;

import java.util.Map;
import java.util.function.Supplier;

public class EqlSearchTask extends CancellableTask {
    private EqlSearchProgressListener progressListener = EqlSearchProgressListener.NOOP;

    private final Supplier<String> descriptionSupplier;

    public EqlSearchTask(long id, String type, String action, Supplier<String> descriptionSupplier, TaskId parentTaskId,
                         Map<String, String> headers) {
        super(id, type, action, null, parentTaskId, headers);
        this.descriptionSupplier = descriptionSupplier;
    }

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
        return true;
    }

    @Override
    public String getDescription() {
        return descriptionSupplier.get();
    }

    /**
     * Attach a {@link EqlSearchProgressListener} to this task.
     */
    public void setProgressListener(EqlSearchProgressListener progressListener) {
        this.progressListener = progressListener;
    }

    /**
     * Return the {@link EqlSearchProgressListener} attached to this task.
     */
    public EqlSearchProgressListener getProgressListener() {
        return progressListener;
    }
}
