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

import static java.util.Map.entry;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.SortOrderAnalyticsEventField.SORT_ORDER_DIRECTION_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.SortOrderAnalyticsEventField.SORT_ORDER_NAME_FIELD;

public class SortOrderAnalyticsEventFieldTests extends AnalyticsEventFieldParserTestCase<String> {
    @Override
    public List<String> requiredFields() {
        return Collections.singletonList(SORT_ORDER_NAME_FIELD.getPreferredName());
    }

    @Override
    protected Map<String, String> createTestInstance() {
        return new HashMap<>(randomEventSearchSortOrderField());
    }

    @Override
    protected ContextParser<AnalyticsEvent.Context, Map<String, String>> parser() {
        return SortOrderAnalyticsEventField::fromXContent;
    }

    public static Map<String, String> randomEventSearchSortOrderField() {
        return Map.ofEntries(
            entry(SORT_ORDER_NAME_FIELD.getPreferredName(), randomIdentifier()),
            entry(SORT_ORDER_DIRECTION_FIELD.getPreferredName(), randomIdentifier())
        );
    }
}
