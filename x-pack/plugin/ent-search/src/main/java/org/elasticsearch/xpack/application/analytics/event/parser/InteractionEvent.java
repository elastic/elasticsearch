/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event.parser;

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsContext;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent;

import java.io.IOException;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;

public class InteractionEvent {

    public enum InteractionType {
        CLICK("click");

        private final String typeName;

        InteractionType(String typeName) {
            this.typeName = typeName;
        }

        public String toString() {
            return typeName.toLowerCase(Locale.ROOT);
        }
    }

    public static final ParseField INTERACTION_FIELD = new ParseField("interaction");
    private static final ParseField INTERACTION_TYPE_FIELD = new ParseField("type");

    private static final ConstructingObjectParser<InteractionType, Void> INTERACTION_FIELD_PARSER = new ConstructingObjectParser<>(
        "event_interaction_data",
        false,
        a -> (InteractionType) a[0]
    );

    private static final ConstructingObjectParser<AnalyticsEvent, AnalyticsContext> PARSER = new ConstructingObjectParser<>(
        "interaction_event",
        false,
        (p, c) -> {
            Map<String, Object> payload = new MapBuilder<String, Object>().put(
                INTERACTION_FIELD.getPreferredName(),
                Collections.singletonMap(INTERACTION_FIELD.getPreferredName(), p[0])
            )
                .put(SessionData.SESSION_FIELD.getPreferredName(), p[1])
                .put(UserData.USER_FIELD.getPreferredName(), p[2])
                .put(PageData.PAGE_FIELD.getPreferredName(), p[3])
                .put(SearchData.SEARCH_FIELD.getPreferredName(), p[4])
                .immutableMap();

            return new AnalyticsEvent(c, payload);
        }
    );

    static {
        INTERACTION_FIELD_PARSER.declareString(
            ConstructingObjectParser.constructorArg(),
            s -> InteractionType.valueOf(s.toUpperCase(Locale.ROOT)),
            INTERACTION_TYPE_FIELD
        );

        PARSER.declareObject(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> INTERACTION_FIELD_PARSER.parse(p, null),
            INTERACTION_FIELD
        );
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> SessionData.parse(p), SessionData.SESSION_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> UserData.parse(p), UserData.USER_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> PageData.parse(p), PageData.PAGE_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> SearchData.parse(p), SearchData.SEARCH_FIELD);
    }

    public static AnalyticsEvent parse(XContentParser parser, AnalyticsContext context) throws IOException {
        return PARSER.parse(parser, context);
    }
}
