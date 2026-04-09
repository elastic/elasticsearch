/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.request.RequestTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RequestManagerTests {
    public static RequestManager createMockWithRateLimitingDisabled(RequestSender requestSender, String inferenceEntityId) {
        return createMock(requestSender, inferenceEntityId, new RateLimitSettings(1, TimeUnit.MINUTES, false));
    }

    public static RequestManager createMockWithRateLimitingDisabled(String inferenceEntityId) {
        return createMock(mock(RequestSender.class), inferenceEntityId, new RateLimitSettings(1, TimeUnit.MINUTES, false));
    }

    public static RequestManager createMockWithRateLimitingEnabled() {
        return createMockWithRateLimitingEnabled(mock(RequestSender.class));
    }

    public static RequestManager createMockWithRateLimitingEnabled(String inferenceEntityId) {
        return createMockWithRateLimitingEnabled(mock(RequestSender.class), inferenceEntityId);
    }

    public static RequestManager createMockWithRateLimitingEnabled(RequestSender requestSender) {
        return createMock(requestSender, "id", new RateLimitSettings(1, TimeUnit.MINUTES, true));
    }

    public static RequestManager createMockWithRateLimitingEnabled(RequestSender requestSender, String inferenceEntityId) {
        return createMock(requestSender, inferenceEntityId, new RateLimitSettings(1, TimeUnit.MINUTES, true));
    }

    public static RequestManager createMock(RequestSender requestSender, String inferenceEntityId, RateLimitSettings settings) {
        var mockManager = mock(RequestManager.class);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<InferenceServiceResults> listener = (ActionListener<InferenceServiceResults>) invocation.getArguments()[3];
            requestSender.send(
                mock(Logger.class),
                RequestTests.mockRequest(inferenceEntityId),
                () -> false,
                mock(ResponseHandler.class),
                listener
            );

            return Void.TYPE;
        }).when(mockManager).execute(any(), any(), any(), any());

        // just return something consistent so the hashing works
        when(mockManager.rateLimitGrouping()).thenReturn(inferenceEntityId);

        when(mockManager.rateLimitSettings()).thenReturn(settings);
        when(mockManager.inferenceEntityId()).thenReturn(inferenceEntityId);

        return mockManager;
    }
}
