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

public class CCMAuthenticationApplierFactoryTests extends ESTestCase {

    public static CCMAuthenticationApplierFactory createNoopApplierFactory() {
        var mockFactory = mock(CCMAuthenticationApplierFactory.class);
        doAnswer(invocation -> {
            ActionListener<CCMAuthenticationApplierFactory.AuthApplier> listener = invocation.getArgument(0);
            listener.onResponse(CCMAuthenticationApplierFactory.NOOP_APPLIER);
            return Void.TYPE;
        }).when(mockFactory).getAuthenticationApplier(any());

        return mockFactory;
    }
}
