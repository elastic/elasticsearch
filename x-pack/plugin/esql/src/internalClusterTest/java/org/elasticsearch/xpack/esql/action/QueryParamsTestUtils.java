/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.parser.QueryParams;

import java.io.IOException;

public final class QueryParamsTestUtils {
    private QueryParamsTestUtils() {}

    /**
     * Parses the {@code params} array of a {@code _query} request body the way the REST layer does, so tests that run
     * queries through the transport client can declare parameters in their REST form.
     */
    public static QueryParams parseParams(String paramsJson) throws IOException {
        String body = "{\"params\": " + paramsJson + "}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, body)) {
            return RequestXContent.parseSync(parser).params();
        }
    }
}
