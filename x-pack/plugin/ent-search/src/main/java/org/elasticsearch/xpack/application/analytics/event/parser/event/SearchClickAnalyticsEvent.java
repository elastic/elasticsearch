/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event.parser.event;

import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent;
import org.elasticsearch.xpack.application.analytics.event.parser.field.DocumentAnalyticsEventField;
import org.elasticsearch.xpack.application.analytics.event.parser.field.PageAnalyticsEventField;
import org.elasticsearch.xpack.application.analytics.event.parser.field.SearchAnalyticsEventField;
import org.elasticsearch.xpack.application.analytics.event.parser.field.SessionAnalyticsEventField;
import org.elasticsearch.xpack.application.analytics.event.parser.field.UserAnalyticsEventField;

import java.io.IOException;

import static org.elasticsearch.xpack.application.analytics.event.parser.field.DocumentAnalyticsEventField.DOCUMENT_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.PageAnalyticsEventField.PAGE_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.SearchAnalyticsEventField.SEARCH_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.SessionAnalyticsEventField.SESSION_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.UserAnalyticsEventField.USER_FIELD;

/**
 * @deprecated in 9.0
 */
@Deprecated
@UpdateForV10(owner = UpdateForV10.Owner.ENTERPRISE_SEARCH)
public class SearchClickAnalyticsEvent {

    private static final ObjectParser<AnalyticsEvent.Builder, AnalyticsEvent.Context> PARSER = ObjectParser.fromBuilder(
        "search_click_event",
        AnalyticsEvent::builder
    );

    static {
        PARSER.declareObject((b, v) -> b.withField(SESSION_FIELD, v), SessionAnalyticsEventField::fromXContent, SESSION_FIELD);
        PARSER.declareObject((b, v) -> b.withField(USER_FIELD, v), UserAnalyticsEventField::fromXContent, USER_FIELD);
        PARSER.declareObject((b, v) -> b.withField(SEARCH_FIELD, v), SearchAnalyticsEventField::fromXContent, SEARCH_FIELD);
        PARSER.declareObject((b, v) -> b.withField(PAGE_FIELD, v), PageAnalyticsEventField::fromXContent, PAGE_FIELD);
        PARSER.declareObject((b, v) -> b.withField(DOCUMENT_FIELD, v), DocumentAnalyticsEventField::fromXContent, DOCUMENT_FIELD);

        PARSER.declareRequiredFieldSet(SESSION_FIELD.getPreferredName());
        PARSER.declareRequiredFieldSet(USER_FIELD.getPreferredName());
        PARSER.declareRequiredFieldSet(SEARCH_FIELD.getPreferredName());
        PARSER.declareRequiredFieldSet(DOCUMENT_FIELD.getPreferredName(), PAGE_FIELD.getPreferredName());
    }

    private SearchClickAnalyticsEvent() {}

    public static AnalyticsEvent fromXContent(XContentParser parser, AnalyticsEvent.Context context) throws IOException {
        return PARSER.parse(parser, context).build();
    }
}
