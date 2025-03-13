/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event.parser.field;

import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.application.analytics.event.parser.field.SessionAnalyticsEventField.SESSION_ID_FIELD;

public class SessionAnalyticsEventFieldTests extends AnalyticsEventFieldParserTestCase<String> {
    @Override
    public List<String> requiredFields() {
        return Collections.singletonList(SESSION_ID_FIELD.getPreferredName());
    }

    @Override
    protected Map<String, String> createTestInstance() {
        return new HashMap<>(randomEventSessionField());
    }

    @Override
    protected ContextParser<AnalyticsEvent.Context, Map<String, String>> parser() {
        return SessionAnalyticsEventField::fromXContent;
    }

    public static Map<String, String> randomEventSessionField() {
        return Map.of(SESSION_ID_FIELD.getPreferredName(), randomIdentifier());
    }
}
