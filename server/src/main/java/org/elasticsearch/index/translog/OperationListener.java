/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.translog;

import org.elasticsearch.common.io.stream.Writeable;

@FunctionalInterface
public interface OperationListener {

    /**
     * This method is called when a new operation is added to the translog. The Writeable data could be releasable,
     * so it should not be retained beyond the scope of this method.
     *
     * @param data a Writable to get the operation bytes
     * @param seqNo the sequence number of the operation
     * @param location the location written
     */
    void operationAdded(Writeable data, long seqNo, Translog.Location location);
}
