/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event.parser;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class SearchData {
    public static final ParseField SEARCH_FIELD = new ParseField("search");
    private static final ParseField QUERY_FIELD = new ParseField("query");
    private static final ConstructingObjectParser<Map<String, Object>, Void> PARSER = new ConstructingObjectParser<>(
        "event_search_data",
        false,
        params -> Collections.singletonMap(QUERY_FIELD.getPreferredName(), params[0])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), QUERY_FIELD);
    }

    public static Map<String, Object> parse(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
