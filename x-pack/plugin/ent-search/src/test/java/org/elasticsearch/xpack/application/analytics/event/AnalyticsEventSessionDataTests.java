/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xcontent.XContentParseException;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSessionData.SESSION_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventSessionData.SESSION_ID_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.convertMapToJson;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.randomEventSessionData;

public class AnalyticsEventSessionDataTests extends AbstractEventDataTestCase<AnalyticsEventSessionData> {

    public void testFromXContentWhenSessionIdIsBlank() {
        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder().put(SESSION_ID_FIELD.getPreferredName(), "").map();

        Exception e = expectThrows(
            XContentParseException.class,
            LoggerMessageFormat.format("[{}] failed to parse field [{}]", SESSION_FIELD, SESSION_ID_FIELD),
            () -> parseJson(convertMapToJson(jsonMap))
        );

        assertEquals(IllegalArgumentException.class, e.getCause().getClass());
        assertEquals(LoggerMessageFormat.format("field [{}] can't be blank", SESSION_ID_FIELD), e.getCause().getMessage());
    }

    @Override
    protected ContextParser<AnalyticsEvent.Context, AnalyticsEventSessionData> parser() {
        return AnalyticsEventSessionData::fromXContent;
    }

    @Override
    protected void assertXContentData(AnalyticsEventSessionData session, Map<String, Object> objectAsMap) {
        assertEquals(1, objectAsMap.size());
        assertTrue(objectAsMap.containsKey(SESSION_ID_FIELD.getPreferredName()));
        assertEquals(session.id(), objectAsMap.get(SESSION_ID_FIELD.getPreferredName()));
    }

    @Override
    protected List<String> requiredFields() {
        return Collections.singletonList(SESSION_ID_FIELD.getPreferredName());
    }

    @Override
    protected Writeable.Reader<AnalyticsEventSessionData> instanceReader() {
        return AnalyticsEventSessionData::new;
    }

    @Override
    protected AnalyticsEventSessionData createTestInstance() {
        return randomEventSessionData();
    }

    @Override
    protected AnalyticsEventSessionData mutateInstance(AnalyticsEventSessionData instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
