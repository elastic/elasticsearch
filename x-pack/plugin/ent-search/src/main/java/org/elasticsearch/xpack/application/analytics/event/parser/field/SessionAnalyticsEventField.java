
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event.parser.field;

import org.elasticsearch.common.Strings;
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

    public static final ParseField CLIENT_ADDRESS_FIELD = new ParseField("ip");

    public static final ParseField USER_AGENT_FIELD = new ParseField("user_agent");

    private static final ObjectParser<MapBuilder<String, String>, AnalyticsEvent.Context> PARSER = ObjectParser.fromBuilder(
        SESSION_FIELD.getPreferredName(),
        (c) -> {
            MapBuilder<String, String> mapBuilder = MapBuilder.newMapBuilder();

            if (Strings.isNullOrBlank(c.clientAddress()) == false) {
                mapBuilder.put(CLIENT_ADDRESS_FIELD.getPreferredName(), c.clientAddress());
            }

            if (Strings.isNullOrBlank(c.userAgent()) == false) {
                mapBuilder.put(USER_AGENT_FIELD.getPreferredName(), c.userAgent());
            }

            return mapBuilder;
        }
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
