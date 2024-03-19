/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.http.client.protocol.HttpClientContext;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.request.RequestTests;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExecutableRequestCreatorTests {
    public static ExecutableRequestCreator createMock() {
        var mockCreator = mock(ExecutableRequestCreator.class);
        when(mockCreator.create(anyList(), any(), any(), any(), any())).thenReturn(() -> {});

        return mockCreator;
    }

    public static ExecutableRequestCreator createMock(RequestSender requestSender) {
        return createMock(requestSender, "id");
    }

    public static ExecutableRequestCreator createMock(RequestSender requestSender, String modelId) {
        var mockCreator = mock(ExecutableRequestCreator.class);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<InferenceServiceResults> listener = (ActionListener<InferenceServiceResults>) invocation.getArguments()[4];
            return (Runnable) () -> requestSender.send(
                mock(Logger.class),
                RequestTests.mockRequest(modelId),
                HttpClientContext.create(),
                () -> false,
                mock(ResponseHandler.class),
                listener
            );
        }).when(mockCreator).create(anyList(), any(), any(), any(), any());

        return mockCreator;
    }
}
