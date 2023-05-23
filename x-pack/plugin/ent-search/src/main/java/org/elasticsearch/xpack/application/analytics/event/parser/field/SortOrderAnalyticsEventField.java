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

public class SortOrderAnalyticsEventField {

    public static ParseField SORT_FIELD = new ParseField("sort");

    public static ParseField SORT_ORDER_NAME_FIELD = new ParseField("name");

    public static ParseField SORT_ORDER_DIRECTION_FIELD = new ParseField("direction");

    private static final ObjectParser<MapBuilder<String, String>, AnalyticsEvent.Context> PARSER = new ObjectParser<>(
        SORT_FIELD.getPreferredName(),
        MapBuilder::newMapBuilder
    );

    static {
        PARSER.declareString(
            (b, v) -> b.put(SORT_ORDER_NAME_FIELD.getPreferredName(), requireNonBlank(v, "field [name] can't be blank")),
            SORT_ORDER_NAME_FIELD
        );
        PARSER.declareString((b, v) -> b.put(SORT_ORDER_DIRECTION_FIELD.getPreferredName(), v), SORT_ORDER_DIRECTION_FIELD);

        PARSER.declareRequiredFieldSet(SORT_ORDER_NAME_FIELD.getPreferredName());
    }

    private SortOrderAnalyticsEventField() {}

    public static Map<String, String> fromXContent(XContentParser parser, AnalyticsEvent.Context context) throws IOException {
        return PARSER.parse(parser, context).immutableMap();
    }
}
