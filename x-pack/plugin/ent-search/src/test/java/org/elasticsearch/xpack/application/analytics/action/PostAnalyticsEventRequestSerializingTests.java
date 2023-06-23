/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.action;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.randomInetAddress;
import static org.mockito.Mockito.mock;

public class PostAnalyticsEventRequestSerializingTests extends AbstractWireSerializingTestCase<PostAnalyticsEventAction.Request> {

    public void testValidate() {
        assertNull(createTestInstance().validate());
    }

    private static String randomEventType() {
        return randomFrom(AnalyticsEvent.Type.values()).toString().toLowerCase(Locale.ROOT);
    }

    public void testValidateInvalidEventTypes() {
        List<String> invalidEventTypes = List.of(randomIdentifier(), randomEventType().toUpperCase(Locale.ROOT));

        for (String eventType : invalidEventTypes) {
            PostAnalyticsEventAction.Request request = PostAnalyticsEventAction.Request.builder(
                randomIdentifier(),
                eventType,
                randomFrom(XContentType.values()),
                mock(BytesReference.class)
            ).eventTime(randomLong()).debug(randomBoolean()).clientAddress(randomInetAddress()).headers(randomHeaders()).request();

            ValidationException e = request.validate();
            assertNotNull(e);
            assertEquals(Collections.singletonList("invalid event type: [" + eventType + "]"), e.validationErrors());
        }
    }

    @Override
    protected Writeable.Reader<PostAnalyticsEventAction.Request> instanceReader() {
        return PostAnalyticsEventAction.Request::new;
    }

    private Map<String, List<String>> randomHeaders() {
        Map<String, List<String>> headersMap = randomMap(
            0,
            10,
            () -> new Tuple<>(randomAlphaOfLengthBetween(1, 10), randomList(0, 5, () -> randomAlphaOfLengthBetween(1, 10)))
        );
        if (randomBoolean()) {
            headersMap.put("User-Agent", List.of(randomAlphaOfLengthBetween(10, 100)));
        }
        return headersMap;
    }

    @Override
    protected PostAnalyticsEventAction.Request mutateInstance(PostAnalyticsEventAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected PostAnalyticsEventAction.Request createTestInstance() {
        return PostAnalyticsEventAction.Request.builder(
            randomIdentifier(),
            randomEventType(),
            randomFrom(XContentType.values()),
            new BytesArray(randomByteArrayOfLength(20))
        ).eventTime(randomLong()).debug(randomBoolean()).headers(randomHeaders()).clientAddress(randomInetAddress()).request();
    }
}
