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

import static org.elasticsearch.xpack.application.analytics.event.parser.field.AnalyticsEventSearchField.SEARCH_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.AnalyticsEventSearchFieldTests.randomEventSearchField;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.AnalyticsEventSessionField.SESSION_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.AnalyticsEventSessionFieldTests.randomEventSessionField;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.AnalyticsEventUserField.USER_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.AnalyticsEventUserFieldTests.randomEventUserField;

public class AnalyticsEventSearchTests extends AnalyticsEventParserTestCase {
    @Override
    protected ContextParser<AnalyticsEvent.Context, AnalyticsEvent> parser() {
        return AnalyticsEventSearch::fromXContent;
    }

    @Override
    protected AnalyticsEvent createTestInstance() throws IOException {
        return randomSearchEvent();
    }

    @Override
    protected List<String> requiredFields() {
        return Stream.of(SESSION_FIELD, USER_FIELD, SEARCH_FIELD).map(ParseField::getPreferredName).collect(Collectors.toList());
    }

    @Override
    protected String parserName() {
        return "search_event";
    }

    public static AnalyticsEvent randomSearchEvent() throws IOException {
        MapBuilder<String, Object> payloadBuilder = MapBuilder.newMapBuilder();

        payloadBuilder.put(SESSION_FIELD.getPreferredName(), randomEventSessionField());
        payloadBuilder.put(USER_FIELD.getPreferredName(), randomEventUserField());
        payloadBuilder.put(SEARCH_FIELD.getPreferredName(), randomEventSearchField());

        return randomAnalyticsEvent(AnalyticsEvent.Type.SEARCH, payloadBuilder.map());
    }
}
