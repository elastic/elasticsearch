/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.ccm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class CCMServiceTests extends ESTestCase {

    public static CCMService createMockCCMService(boolean enabled) {
        var mockService = mock(CCMService.class);

        doAnswer(invocation -> {
            ActionListener<Boolean> listener = invocation.getArgument(0);
            listener.onResponse(enabled);
            return Void.TYPE;
        }).when(mockService).isEnabled(any());

        return mockService;
    }
}
