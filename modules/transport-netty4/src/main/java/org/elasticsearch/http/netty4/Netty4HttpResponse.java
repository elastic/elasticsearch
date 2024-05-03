/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

/**
 * Super-interface for responses handled by the Netty4 HTTP transport.
 */
public sealed interface Netty4HttpResponse permits Netty4FullHttpResponse, Netty4ChunkedHttpResponse {
    /**
     * @return The sequence number for the request which corresponds with this response, for making sure that we send responses to pipelined
     * requests in the correct order.
     */
    int getSequence();
}
