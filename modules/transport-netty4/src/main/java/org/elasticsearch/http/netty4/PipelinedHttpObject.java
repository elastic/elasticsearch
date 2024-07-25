/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

public sealed interface PipelinedHttpObject permits PipelinedFullHttpRequest, PipelinedHttpContent, PipelinedHttpRequest,
    PipelinedHttpRequestPart, PipelinedLastHttpContent {

    /**
     * HTTP request sequence number, indicates order of arrival within same channel (connection).
     * All parts of a single HTTP request - {@link PipelinedHttpRequest} and {@link PipelinedHttpContent} - has same sequence number.
     */
    int sequence();
}
