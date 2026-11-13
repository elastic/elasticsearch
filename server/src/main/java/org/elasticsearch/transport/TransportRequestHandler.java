/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.tasks.Task;

public interface TransportRequestHandler<T extends TransportRequest> {

    /**
     * Handles {@code request} and eventually completes {@code channel}.
     * <p>
     * The transport holds a reference to a ref-counted {@code request} only until this method returns. A handler that uses the request
     * after returning, for instance in an asynchronous operation, must take its own reference first and release it when it no longer
     * needs the request. For a request sent to the local node this is the sender's own instance, which the sender may release as soon
     * as the response handler completes.
     */
    void messageReceived(T request, TransportChannel channel, Task task) throws Exception;
}
