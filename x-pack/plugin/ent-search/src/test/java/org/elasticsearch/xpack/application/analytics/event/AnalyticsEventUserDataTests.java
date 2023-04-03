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

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.convertMapToJson;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.randomEventUserData;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventUserData.USER_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventUserData.USER_ID_FIELD;

public class AnalyticsEventUserDataTests extends AbstractEventDataTestCase<AnalyticsEventUserData> {
    public void testFromXContentWhenSessionIdIsBlank() {
        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder().put(USER_ID_FIELD.getPreferredName(), "").map();

        Exception e = expectThrows(
            XContentParseException.class,
            LoggerMessageFormat.format("[{}}] failed to parse field [{}}]", USER_FIELD, USER_ID_FIELD),
            () -> parseJson(convertMapToJson(jsonMap))
        );

        assertEquals(IllegalArgumentException.class, e.getCause().getClass());
        assertEquals(LoggerMessageFormat.format("field [{}] can't be blank", USER_ID_FIELD), e.getCause().getMessage());
    }

    @Override
    protected ContextParser<AnalyticsEvent.Context, AnalyticsEventUserData> parser() {
        return AnalyticsEventUserData::fromXContent;
    }

    @Override
    protected void assertXContentData(AnalyticsEventUserData user, Map<String, Object> objectAsMap) {
        assertEquals(1, objectAsMap.size());
        assertTrue(objectAsMap.containsKey(USER_ID_FIELD.getPreferredName()));
        assertEquals(user.id(), objectAsMap.get(USER_ID_FIELD.getPreferredName()));
    }

    @Override
    protected List<String> requiredFields() {
        return Collections.singletonList(USER_ID_FIELD.getPreferredName());
    }

    @Override
    protected Writeable.Reader<AnalyticsEventUserData> instanceReader() {
        return AnalyticsEventUserData::new;
    }

    @Override
    protected AnalyticsEventUserData createTestInstance() {
        return randomEventUserData();
    }

    @Override
    protected AnalyticsEventUserData mutateInstance(AnalyticsEventUserData instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
