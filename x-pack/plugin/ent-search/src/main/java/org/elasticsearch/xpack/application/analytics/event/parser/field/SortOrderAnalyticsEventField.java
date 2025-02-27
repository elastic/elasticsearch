/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event.parser.field;

import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.Strings.requireNonBlank;

/**
 * @deprecated in 9.0
 */
@Deprecated
@UpdateForV10(owner = UpdateForV10.Owner.ENTERPRISE_SEARCH)
public class SortOrderAnalyticsEventField {

    public static final ParseField SORT_FIELD = new ParseField("sort");

    public static final ParseField SORT_ORDER_NAME_FIELD = new ParseField("name");

    public static final ParseField SORT_ORDER_DIRECTION_FIELD = new ParseField("direction");

    private static final ObjectParser<Map<String, String>, AnalyticsEvent.Context> PARSER = new ObjectParser<>(
        SORT_FIELD.getPreferredName(),
        HashMap::new
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
        return Map.copyOf(PARSER.parse(parser, context));
    }
}
