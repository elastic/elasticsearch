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

public class JsonArrayPartsEventProcessor extends DelegatingProcessor<HttpResult, Deque<byte[]>> {
    private final JsonArrayPartsEventParser jsonArrayPartsEventParser;

    public JsonArrayPartsEventProcessor(JsonArrayPartsEventParser jsonArrayPartsEventParser) {
        this.jsonArrayPartsEventParser = jsonArrayPartsEventParser;
    }

    @Override
    public void next(HttpResult item) {
        if (item.isBodyEmpty()) {
            upstream().request(1);
            return;
        }

        var response = jsonArrayPartsEventParser.parse(item.body());
        if (response.isEmpty()) {
            upstream().request(1);
            return;
        }

        downstream().onNext(response);
    }
}
