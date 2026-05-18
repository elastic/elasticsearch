/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event.parser.field;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @deprecated in 9.0
 */
@Deprecated
@UpdateForV10(owner = UpdateForV10.Owner.ENTERPRISE_SEARCH)
public class SearchFiltersAnalyticsEventField {
    public static final ParseField SEARCH_FILTERS_FIELD = new ParseField("filters");

    private static final ObjectParser<Map<String, List<String>>, AnalyticsEvent.Context> PARSER = new ObjectParser<>(
        SEARCH_FILTERS_FIELD.getPreferredName(),
        SearchFiltersAnalyticsEventField::parseValue,
        HashMap::new
    );

    private SearchFiltersAnalyticsEventField() {}

    public static Map<String, List<String>> fromXContent(XContentParser parser, AnalyticsEvent.Context context) throws IOException {
        return Map.copyOf(PARSER.parse(parser, context));
    }

    @SuppressWarnings("unchecked")
    private static void parseValue(Map<String, List<String>> builder, String field, Object value) {
        if (value instanceof String) {
            builder.put(field, List.of((String) value));
            return;
        } else if (value instanceof List && ((List<?>) value).stream().allMatch(v -> v instanceof String)) {
            builder.put(field, (List<String>) value);
            return;
        }

        throw new IllegalArgumentException(Strings.format("[%s] must be a string or an array of string.", field));
    }
}
