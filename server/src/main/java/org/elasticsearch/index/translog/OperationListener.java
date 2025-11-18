/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.translog;

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
}
