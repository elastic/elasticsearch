/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public interface TransportFuture<V> extends Future<V> {

    /**
     * Waits if necessary for the computation to complete, and then
     * retrieves its result.
     */
    V txGet();

    /**
     * Waits if necessary for at most the given time for the computation
     * to complete, and then retrieves its result, if available.
     */
    V txGet(long timeout, TimeUnit unit);
}
