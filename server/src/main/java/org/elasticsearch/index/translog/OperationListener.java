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
     * {@link Translog.Operation} ({@code minSeqNo == maxSeqNo}) or an {@link IndexOperationBatch.TranslogRecord}
     * (one sequence number per replayable row). A record's operations always occupy the contiguous range
     * {@code [minSeqNo, maxSeqNo]}, so the two bounds identify every operation the record carries.
     *
     * @param operation the serialized record added to the translog
     * @param minSeqNo the lowest sequence number the record carries
     * @param maxSeqNo the highest sequence number the record carries (inclusive)
     * @param location the location written
     */
    void recordAdded(Translog.Serialized operation, long minSeqNo, long maxSeqNo, Translog.Location location);
}
