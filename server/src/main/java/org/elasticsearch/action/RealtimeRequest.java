/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

/**
 * Indicates that a request can execute in realtime (reads from the translog).
 * All {@link ActionRequest} that are realtime should implement this interface.
 */
public interface RealtimeRequest {

    /**
     * @param realtime Controls whether this request should be realtime by reading from the translog.
     */
    <R extends RealtimeRequest> R realtime(boolean realtime);

}
