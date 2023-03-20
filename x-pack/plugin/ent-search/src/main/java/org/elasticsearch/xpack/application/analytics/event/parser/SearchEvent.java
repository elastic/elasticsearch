/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event.parser;

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsContext;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent;

import java.io.IOException;
import java.util.Map;

public class SearchEvent {
    private static final ConstructingObjectParser<AnalyticsEvent, AnalyticsContext> PARSER = new ConstructingObjectParser<>(
        "search_event",
        false,
        (p, c) -> {
            Map<String, Object> payload = new MapBuilder<String, Object>().put(SessionData.SESSION_FIELD.getPreferredName(), p[0])
                .put(UserData.USER_FIELD.getPreferredName(), p[1])
                .put(SearchData.SEARCH_FIELD.getPreferredName(), p[2])
                .immutableMap();

            return new AnalyticsEvent(c, payload);
        }
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> SessionData.parse(p), SessionData.SESSION_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> UserData.parse(p), UserData.USER_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> SearchData.parse(p), SearchData.SEARCH_FIELD);
    }

    public static AnalyticsEvent parse(XContentParser parser, AnalyticsContext context) throws IOException {
        return PARSER.parse(parser, context);
    }
}
