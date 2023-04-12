/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event.parser.field;

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.application.analytics.event.parser.field.UserAnalyticsEventField.USER_ID_FIELD;

public class UserAnalyticsEventFieldTests extends AnalyticsEventFieldParserTestCase<String> {
    @Override
    public List<String> requiredFields() {
        return Collections.singletonList(USER_ID_FIELD.getPreferredName());
    }

    @Override
    protected Map<String, String> createTestInstance() {
        return randomEventUserField();
    }

    @Override
    protected ContextParser<AnalyticsEvent.Context, Map<String, String>> parser() {
        return UserAnalyticsEventField::fromXContent;
    }

    public static Map<String, String> randomEventUserField() {
        return MapBuilder.<String, String>newMapBuilder().put(USER_ID_FIELD.getPreferredName(), randomIdentifier()).map();
    }
}
