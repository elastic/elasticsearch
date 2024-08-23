/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import org.elasticsearch.http.HttpRequest;

public sealed interface Netty4HttpRequest extends HttpRequest permits Netty4AbstractHttpRequest, Netty4FullHttpRequest,
    Netty4HttpRequestException, Netty4HttpStreamRequest {

    int sequence();

    io.netty.handler.codec.http.HttpRequest nettyRequest();
}
