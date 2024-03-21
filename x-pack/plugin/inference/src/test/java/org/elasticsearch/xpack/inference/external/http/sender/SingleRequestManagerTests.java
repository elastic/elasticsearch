/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.http.client.protocol.HttpClientContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.retry.RetryingHttpSender;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class SingleRequestManagerTests extends ESTestCase {
    public void testExecute_DoesNotCallRequestCreatorCreate_WhenInputIsNull() {
        var requestCreator = mock(ExecutableRequestCreator.class);
        var request = mock(InferenceRequest.class);
        when(request.getRequestCreator()).thenReturn(requestCreator);

        new SingleRequestManager(mock(RetryingHttpSender.class)).execute(mock(InferenceRequest.class), HttpClientContext.create());
        verifyNoInteractions(requestCreator);
    }
}
