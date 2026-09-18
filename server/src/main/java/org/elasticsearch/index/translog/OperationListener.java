/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.translog;

import org.elasticsearch.index.engine.IndexOperationBatch;

@FunctionalInterface
public interface OperationListener {

    /**
     * This method is called when a new {@link Translog.Record} is added to the translog: either a single
     * {@link Translog.Operation} (one sequence number) or an {@link IndexOperationBatch.TranslogRecord}
     * (one sequence number per replayable row).
     *
     * @param operation the serialized record added to the translog
     * @param seqNos the sequence numbers of the operations the record carries
     * @param location the location written
     */
    void recordAdded(Translog.Serialized operation, long[] seqNos, Translog.Location location);
}
