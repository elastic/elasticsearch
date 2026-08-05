/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.translog;

import java.util.List;

@FunctionalInterface
public interface OperationListener {

    /**
     * This method is called when a new operation is added to the translog.
     *
     * @param operation the serialized operation added to the translog
     * @param seqNo the sequence number of the operation
     * @param location the location written
     */
    void operationAdded(Translog.Serialized operation, long seqNo, Translog.Location location);

    /**
     * This method is called when a {@link Translog.IndexBatch} record is added to the translog.
     * The default implementation is a no-op so that non-batch-aware listeners ({@link TranslogConfig#NOOP_OPERATION_LISTENER})
     * are unaffected; batch-aware listeners override it to observe every operation in the batch.
     */
    default void batchAdded(Translog.Serialized operation, List<Long> seqNos, Translog.Location location) {}
}
