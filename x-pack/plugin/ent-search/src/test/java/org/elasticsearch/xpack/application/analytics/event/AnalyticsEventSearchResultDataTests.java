/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ContextParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSearchResultData.SEARCH_RESULTS_TOTAL_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSearchResultData.SEARCH_RESULT_ITEMS_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.randomEventSearchResults;

public class AnalyticsEventSearchResultDataTests extends AbstractEventDataTestCase<AnalyticsEventSearchResultData> {

    @Override
    protected Writeable.Reader<AnalyticsEventSearchResultData> instanceReader() {
        return AnalyticsEventSearchResultData::new;
    }

    @Override
    protected AnalyticsEventSearchResultData createTestInstance() {
        return randomEventSearchResults();
    }

    @Override
    protected AnalyticsEventSearchResultData mutateInstance(AnalyticsEventSearchResultData instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected ContextParser<AnalyticsEvent.Context, AnalyticsEventSearchResultData> parser() {
        return AnalyticsEventSearchResultData::fromXContent;
    }

    @Override
    protected void assertXContentData(AnalyticsEventSearchResultData searchResult, Map<String, Object> objectAsMap) {
        assertEquals(2, objectAsMap.size());

        assertEquals(searchResult.totalResults(), objectAsMap.get(SEARCH_RESULTS_TOTAL_FIELD.getPreferredName()));
        assertEquals(searchResult.items().size(), ((List<?>) objectAsMap.get(SEARCH_RESULT_ITEMS_FIELD.getPreferredName())).size());
    }

    @Override
    protected List<String> requiredFields() {
        return Collections.emptyList();
    }
}
