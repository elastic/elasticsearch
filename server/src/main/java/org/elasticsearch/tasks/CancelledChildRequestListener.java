/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.tasks;

/**
 * Notified when a child request is cancelled on this node, also when its task has already completed, for example because the parent
 * could not read its response. Lets the node release what the child left for the parent to use.
 *
 * @see TaskManager#registerCancelledChildRequestListener(CancelledChildRequestListener)
 */
@FunctionalInterface
public interface CancelledChildRequestListener {
    /**
     * Called with the parent task and request id of the cancelled child request. Usually runs on a transport thread, so it must not
     * block, do IO, or throw.
     */
    void onCancelled(TaskId parentTaskId, long childRequestId);
}
