/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event.parser.field;

import org.elasticsearch.core.Strings;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @deprecated in 9.0
 */
@Deprecated
@UpdateForV10(owner = UpdateForV10.Owner.ENTERPRISE_SEARCH)
public class PaginationAnalyticsEventField {

    public static final ParseField PAGINATION_FIELD = new ParseField("page");

    public static final ParseField CURRENT_PAGE_FIELD = new ParseField("current");

    public static final ParseField PAGE_SIZE_FIELD = new ParseField("size");

    private static final ObjectParser<Map<String, Integer>, AnalyticsEvent.Context> PARSER = new ObjectParser<>(
        PAGINATION_FIELD.getPreferredName(),
        HashMap::new
    );

    private static int requirePositiveInt(int i, String field) {
        if (i < 0) throw new IllegalArgumentException(Strings.format("field [%s] must be positive", field));
        return i;
    }

    static {
        PARSER.declareInt(
            (b, v) -> b.put(CURRENT_PAGE_FIELD.getPreferredName(), requirePositiveInt(v, CURRENT_PAGE_FIELD.getPreferredName())),
            CURRENT_PAGE_FIELD
        );
        PARSER.declareInt(
            (b, v) -> b.put(PAGE_SIZE_FIELD.getPreferredName(), requirePositiveInt(v, PAGE_SIZE_FIELD.getPreferredName())),
            PAGE_SIZE_FIELD
        );

        PARSER.declareRequiredFieldSet(CURRENT_PAGE_FIELD.getPreferredName());
        PARSER.declareRequiredFieldSet(PAGE_SIZE_FIELD.getPreferredName());
    }

    private PaginationAnalyticsEventField() {}

    public static Map<String, Integer> fromXContent(XContentParser parser, AnalyticsEvent.Context context) throws IOException {
        return Map.copyOf(PARSER.parse(parser, context));
    }
}
