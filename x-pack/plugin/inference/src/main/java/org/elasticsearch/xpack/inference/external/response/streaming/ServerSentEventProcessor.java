/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.streaming;

import org.elasticsearch.xpack.inference.common.DelegatingProcessor;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.util.Deque;

public class ServerSentEventProcessor extends DelegatingProcessor<HttpResult, Deque<ServerSentEvent>> {
    private final ServerSentEventParser serverSentEventParser;

    public ServerSentEventProcessor(ServerSentEventParser serverSentEventParser) {
        this.serverSentEventParser = serverSentEventParser;
    }

    @Override
    public void next(HttpResult item) {
        if (item.isBodyEmpty()) {
            // discard empty result and go to the next
            upstream().request(1);
            return;
        }

        var response = serverSentEventParser.parse(item.body());
        if (response.isEmpty()) {
            // discard empty result and go to the next
            upstream().request(1);
            return;
        }

        downstream().onNext(response);
    }
}
