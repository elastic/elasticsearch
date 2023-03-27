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
import java.util.Map;

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSearchData.SEARCH_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSearchData.SEARCH_QUERY_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSearchDataTests.randomEventSearchData;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSessionDataTests.randomEventSessionData;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventUserDataTests.randomEventUserData;

public class AnalyticsEventSearchTests extends AbstractEventTestCase<AnalyticsEventSearch> {

    @SuppressWarnings("unchecked")
    public void assertToXContentAdditionalFields(Map<String, Object> eventMap, AnalyticsEventSearch event) {
        // Check search field content.
        assertTrue(eventMap.containsKey(SEARCH_FIELD.getPreferredName()));
        Map<String, String> searchDataMap = (Map<String, String>) eventMap.get(SEARCH_FIELD.getPreferredName());
        assertEquals(event.search().query(), searchDataMap.get(SEARCH_QUERY_FIELD.getPreferredName()));
    }

    @Override
    protected Writeable.Reader<AnalyticsEventSearch> instanceReader() {
        return AnalyticsEventSearch::new;
    }

    @Override
    protected AnalyticsEventSearch createTestInstance() {
        return new AnalyticsEventSearch(
            randomIdentifier(),
            randomLong(),
            randomEventSessionData(),
            randomEventUserData(),
            randomEventSearchData()
        );
    }

    @Override
    protected AnalyticsEventSearch mutateInstance(AnalyticsEventSearch instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected ContextParser<AnalyticsEvent.Context, AnalyticsEventSearch> parser() {
        return AnalyticsEventSearch::fromXContent;
    }
}
