/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event.parser.event;

import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Map.entry;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.DocumentAnalyticsEventField.DOCUMENT_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.DocumentAnalyticsEventFieldTests.randomEventDocumentField;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.PageAnalyticsEventField.PAGE_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.PageAnalyticsEventFieldTests.randomEventPageField;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.SearchAnalyticsEventField.SEARCH_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.SearchAnalyticsEventFieldTests.randomEventSearchField;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.SessionAnalyticsEventField.SESSION_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.SessionAnalyticsEventFieldTests.randomEventSessionField;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.UserAnalyticsEventField.USER_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.UserAnalyticsEventFieldTests.randomEventUserField;

public class SearchClickAnalyticsEventTests extends AnalyticsEventParserTestCase {
    @Override
    protected ContextParser<AnalyticsEvent.Context, AnalyticsEvent> parser() {
        return SearchClickAnalyticsEvent::fromXContent;
    }

    @Override
    protected AnalyticsEvent createTestInstance() throws IOException {
        return randomSearchClickEvent();
    }

    @Override
    protected List<String> requiredFields() {
        return Stream.of(SESSION_FIELD, USER_FIELD, SEARCH_FIELD).map(ParseField::getPreferredName).collect(Collectors.toList());
    }

    @Override
    protected String parserName() {
        return "search_click_event";
    }

    protected Predicate<String> isFieldRequired() {
        return super.isFieldRequired().or(DOCUMENT_FIELD.getPreferredName()::equals).or(PAGE_FIELD.getPreferredName()::equals);
    }

    public static AnalyticsEvent randomSearchClickEvent() throws IOException {
        Map<String, Object> payloadBuilder = Map.ofEntries(
            entry(SESSION_FIELD.getPreferredName(), randomEventSessionField()),
            entry(USER_FIELD.getPreferredName(), randomEventUserField()),
            entry(PAGE_FIELD.getPreferredName(), randomEventPageField()),
            entry(DOCUMENT_FIELD.getPreferredName(), randomEventDocumentField()),
            entry(SEARCH_FIELD.getPreferredName(), randomEventSearchField())
        );

        return randomAnalyticsEvent(AnalyticsEvent.Type.SEARCH_CLICK, payloadBuilder);
    }
}
