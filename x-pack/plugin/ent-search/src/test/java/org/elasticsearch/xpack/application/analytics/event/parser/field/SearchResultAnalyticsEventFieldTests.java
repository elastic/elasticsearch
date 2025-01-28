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
import static org.elasticsearch.xpack.application.analytics.event.parser.field.DocumentAnalyticsEventField.DOCUMENT_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.DocumentAnalyticsEventFieldTests.randomEventDocumentField;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.PageAnalyticsEventField.PAGE_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.SearchResultAnalyticsEventField.SEARCH_RESULTS_TOTAL_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.SearchResultAnalyticsEventField.SEARCH_RESULT_ITEMS_FIELD;

public class SearchResultAnalyticsEventFieldTests extends AnalyticsEventFieldParserTestCase<Object> {
    @Override
    public List<String> requiredFields() {
        return Collections.emptyList();
    }

    @Override
    protected Map<String, Object> createTestInstance() {
        return new HashMap<>(randomEventSearchResultField());
    }

    @Override
    protected ContextParser<AnalyticsEvent.Context, Map<String, Object>> parser() {
        return SearchResultAnalyticsEventField::fromXContent;
    }

    public static Map<String, Object> randomEventSearchResultField() {
        List<?> items = randomList(
            between(1, 10),
            () -> Map.ofEntries(
                entry(DOCUMENT_FIELD.getPreferredName(), randomEventDocumentField()),
                entry(PAGE_FIELD.getPreferredName(), PageAnalyticsEventFieldTests.randomEventPageField())
            )
        );

        return Map.ofEntries(
            entry(SEARCH_RESULTS_TOTAL_FIELD.getPreferredName(), randomNonNegativeInt()),
            entry(SEARCH_RESULT_ITEMS_FIELD.getPreferredName(), items)
        );
    }
}
