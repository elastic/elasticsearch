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

import static org.elasticsearch.xpack.application.analytics.event.parser.field.DocumentAnalyticsEventField.DOCUMENT_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.PageAnalyticsEventField.PAGE_FIELD;

/**
 * @deprecated in 9.0
 */
@Deprecated
@UpdateForV10(owner = UpdateForV10.Owner.ENTERPRISE_SEARCH)
public class SearchResultAnalyticsEventField {
    public static final ParseField SEARCH_RESULTS_TOTAL_FIELD = new ParseField("total_results");

    public static final ParseField SEARCH_RESULT_ITEMS_FIELD = new ParseField("items");

    private static final ObjectParser<Map<String, Object>, AnalyticsEvent.Context> PARSER = new ObjectParser<>(
        "search_results",
        HashMap::new
    );

    private static final ObjectParser<Map<String, Object>, AnalyticsEvent.Context> ITEM_PARSER = new ObjectParser<>(
        "search_results_item",
        HashMap::new
    );

    static {
        PARSER.declareObjectArray(
            (b, v) -> b.put(SEARCH_RESULT_ITEMS_FIELD.getPreferredName(), v),
            (p, c) -> Map.copyOf(ITEM_PARSER.parse(p, c)),
            SEARCH_RESULT_ITEMS_FIELD
        );
        PARSER.declareInt((b, v) -> b.put(SEARCH_RESULTS_TOTAL_FIELD.getPreferredName(), v), SEARCH_RESULTS_TOTAL_FIELD);

        ITEM_PARSER.declareObject(
            (b, v) -> b.put(DOCUMENT_FIELD.getPreferredName(), v),
            DocumentAnalyticsEventField::fromXContent,
            DOCUMENT_FIELD
        );
        ITEM_PARSER.declareObject((b, v) -> b.put(PAGE_FIELD.getPreferredName(), v), PageAnalyticsEventField::fromXContent, PAGE_FIELD);
    }

    private SearchResultAnalyticsEventField() {}

    public static Map<String, Object> fromXContent(XContentParser parser, AnalyticsEvent.Context context) throws IOException {
        return Map.copyOf(PARSER.parse(parser, context));
    }
}
