/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event.parser.event;

import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent;
import org.elasticsearch.xpack.application.analytics.event.parser.field.AnalyticsEventDocumentField;
import org.elasticsearch.xpack.application.analytics.event.parser.field.AnalyticsEventPageField;
import org.elasticsearch.xpack.application.analytics.event.parser.field.AnalyticsEventSearchField;
import org.elasticsearch.xpack.application.analytics.event.parser.field.AnalyticsEventSessionField;
import org.elasticsearch.xpack.application.analytics.event.parser.field.AnalyticsEventUserField;

import java.io.IOException;

import static org.elasticsearch.xpack.application.analytics.event.parser.field.AnalyticsEventDocumentField.DOCUMENT_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.AnalyticsEventPageField.PAGE_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.AnalyticsEventSearchField.SEARCH_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.AnalyticsEventSessionField.SESSION_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.AnalyticsEventUserField.USER_FIELD;

public class AnalyticsEventSearchClick {

    private static final ObjectParser<AnalyticsEvent.Builder, AnalyticsEvent.Context> PARSER = ObjectParser.fromBuilder(
        "search_click_event",
        AnalyticsEvent::builder
    );

    static {
        PARSER.declareObject((b, v) -> b.withField(SESSION_FIELD, v), AnalyticsEventSessionField::fromXContent, SESSION_FIELD);
        PARSER.declareObject((b, v) -> b.withField(USER_FIELD, v), AnalyticsEventUserField::fromXContent, USER_FIELD);
        PARSER.declareObject((b, v) -> b.withField(SEARCH_FIELD, v), AnalyticsEventSearchField::fromXContent, SEARCH_FIELD);
        PARSER.declareObject((b, v) -> b.withField(PAGE_FIELD, v), AnalyticsEventPageField::fromXContent, PAGE_FIELD);
        PARSER.declareObject((b, v) -> b.withField(DOCUMENT_FIELD, v), AnalyticsEventDocumentField::fromXContent, DOCUMENT_FIELD);

        PARSER.declareRequiredFieldSet(SESSION_FIELD.getPreferredName());
        PARSER.declareRequiredFieldSet(USER_FIELD.getPreferredName());
        PARSER.declareRequiredFieldSet(SEARCH_FIELD.getPreferredName());
        PARSER.declareRequiredFieldSet(DOCUMENT_FIELD.getPreferredName(), PAGE_FIELD.getPreferredName());
    }

    private AnalyticsEventSearchClick() {}

    public static AnalyticsEvent fromXContent(XContentParser parser, AnalyticsEvent.Context context) throws IOException {
        return PARSER.parse(parser, context).build();
    }
}
