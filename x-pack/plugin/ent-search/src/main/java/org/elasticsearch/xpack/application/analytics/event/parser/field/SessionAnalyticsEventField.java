
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event.parser.field;

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.Strings.requireNonBlank;

public class SessionAnalyticsEventField {
    public static ParseField SESSION_FIELD = new ParseField("session");

    public static ParseField SESSION_ID_FIELD = new ParseField("id");

    private static final ObjectParser<MapBuilder<String, String>, AnalyticsEvent.Context> PARSER = new ObjectParser<>(
        SESSION_FIELD.getPreferredName(),
        MapBuilder::newMapBuilder
    );

    static {
        PARSER.declareString(
            (b, s) -> b.put(SESSION_ID_FIELD.getPreferredName(), requireNonBlank(s, "field [id] can't be blank")),
            SESSION_ID_FIELD
        );

        PARSER.declareRequiredFieldSet(SESSION_ID_FIELD.getPreferredName());
    }

    private SessionAnalyticsEventField() {}

    public static Map<String, String> fromXContent(XContentParser parser, AnalyticsEvent.Context context) throws IOException {
        return PARSER.parse(parser, context).immutableMap();
    }
}
