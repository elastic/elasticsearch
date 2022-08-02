/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.core;

import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

public final class GetSourceResponse {

    private final Map<String, Object> source;

    public GetSourceResponse(Map<String, Object> source) {
        this.source = source;
    }

    public static GetSourceResponse fromXContent(XContentParser parser) throws IOException {
        return new GetSourceResponse(parser.map());
    }

    public Map<String, Object> getSource() {
        return this.source;
    }

    @Override
    public String toString() {
        return source.toString();
    }
}
