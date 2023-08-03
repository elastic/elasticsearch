/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event.parser.field;

import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PageAnalyticsEventField {
    public static ParseField PAGE_FIELD = new ParseField("page");

    public static ParseField PAGE_URL_FIELD = new ParseField("url");

    public static ParseField PAGE_TITLE_FIELD = new ParseField("title");

    public static ParseField PAGE_REFERRER_FIELD = new ParseField("referrer");

    private static final ObjectParser<Map<String, String>, AnalyticsEvent.Context> PARSER = new ObjectParser<>(
        PAGE_FIELD.getPreferredName(),
        HashMap::new
    );

    static {
        PARSER.declareString((b, v) -> b.put(PAGE_URL_FIELD.getPreferredName(), v), PAGE_URL_FIELD);
        PARSER.declareString((b, v) -> b.put(PAGE_TITLE_FIELD.getPreferredName(), v), PAGE_TITLE_FIELD);
        PARSER.declareString((b, v) -> b.put(PAGE_REFERRER_FIELD.getPreferredName(), v), PAGE_REFERRER_FIELD);

        PARSER.declareRequiredFieldSet(PAGE_URL_FIELD.getPreferredName());
    }

    private PageAnalyticsEventField() {}

    public static Map<String, String> fromXContent(XContentParser parser, AnalyticsEvent.Context context) throws IOException {
        return Map.copyOf(PARSER.parse(parser, context));
    }
}
