/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event.parser.event;

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.application.analytics.event.parser.field.DocumentAnalyticsEventField.DOCUMENT_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.DocumentAnalyticsEventFieldTests.randomEventDocumentField;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.PageAnalyticsEventField.PAGE_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.PageAnalyticsEventFieldTests.randomEventPageField;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.SessionAnalyticsEventField.SESSION_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.SessionAnalyticsEventFieldTests.randomEventSessionField;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.UserAnalyticsEventField.USER_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.UserAnalyticsEventFieldTests.randomEventUserField;

public class PageViewAnalyticsEventTests extends AnalyticsEventParserTestCase {
    @Override
    protected ContextParser<AnalyticsEvent.Context, AnalyticsEvent> parser() {
        return PageViewAnalyticsEvent::fromXContent;
    }

    @Override
    protected AnalyticsEvent createTestInstance() throws IOException {
        return randomPageViewEvent();
    }

    @Override
    protected List<String> requiredFields() {
        return Stream.of(SESSION_FIELD, USER_FIELD, PAGE_FIELD).map(ParseField::getPreferredName).collect(Collectors.toList());
    }

    @Override
    protected String parserName() {
        return "page_view_event";
    }

    public static AnalyticsEvent randomPageViewEvent() throws IOException {
        MapBuilder<String, Object> payloadBuilder = MapBuilder.newMapBuilder();

        payloadBuilder.put(SESSION_FIELD.getPreferredName(), randomEventSessionField());
        payloadBuilder.put(USER_FIELD.getPreferredName(), randomEventUserField());
        payloadBuilder.put(PAGE_FIELD.getPreferredName(), randomEventPageField());
        payloadBuilder.put(DOCUMENT_FIELD.getPreferredName(), randomEventDocumentField());

        return randomAnalyticsEvent(AnalyticsEvent.Type.PAGE_VIEW, payloadBuilder.map());
    }
}
