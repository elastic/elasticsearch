
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
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.Strings.requireNonBlank;

public class UserAnalyticsEventField {
    public static ParseField USER_FIELD = new ParseField("user");

    public static ParseField USER_ID_FIELD = new ParseField("id");

    private static final ObjectParser<Map<String, String>, AnalyticsEvent.Context> PARSER = new ObjectParser<>(
        USER_FIELD.getPreferredName(),
        HashMap::new
    );

    static {
        PARSER.declareString(
            (b, s) -> b.put(USER_ID_FIELD.getPreferredName(), requireNonBlank(s, "field [id] can't be blank")),
            USER_ID_FIELD
        );

        PARSER.declareRequiredFieldSet(USER_ID_FIELD.getPreferredName());
    }

    private UserAnalyticsEventField() {}

    public static Map<String, String> fromXContent(XContentParser parser, AnalyticsEvent.Context context) throws IOException {
        return Map.copyOf(PARSER.parse(parser, context));
    }
}
