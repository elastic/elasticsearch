/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.enrich.EnrichStore;
import org.mockito.Matchers;

import java.util.function.Consumer;

import static org.elasticsearch.xpack.enrich.EnrichPolicyTests.randomEnrichPolicy;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TransportPutEnrichPolicyActionTests extends ESTestCase {

    public void testPutEnricyPolicyAction() {
        EnrichStore store = mock(EnrichStore.class);
        EnrichPolicy policy =  randomEnrichPolicy(XContentType.JSON);

        doAnswer(a -> {
            @SuppressWarnings("unchecked")
            Consumer<Exception> consumer = (Consumer<Exception>) a.getArguments()[2];
            consumer.accept(null);
            return null;
        }).when(store).putPolicy(anyString(), Matchers.any(EnrichPolicy.class), Matchers.any());

        @SuppressWarnings("unchecked")
        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);
        TransportPutEnrichPolicyAction.putPolicy("foo", policy, store, listener);

        verify(listener, times(1)).onResponse(any(AcknowledgedResponse.class));
        verify(listener, times(0)).onFailure(any(Exception.class));
    }

    public void testPutEnricyPolicyAction_storeFailsToWrite() {
        Exception thrown = new Exception("Mock exception");
        EnrichStore store = mock(EnrichStore.class);
        EnrichPolicy policy =  randomEnrichPolicy(XContentType.JSON);

        doAnswer(a -> {
            @SuppressWarnings("unchecked")
            Consumer<Exception> consumer = (Consumer<Exception>) a.getArguments()[2];
            consumer.accept(thrown);
            return null;
        }).when(store).putPolicy(anyString(), Matchers.any(EnrichPolicy.class), Matchers.any());

        @SuppressWarnings("unchecked")
        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);
        TransportPutEnrichPolicyAction.putPolicy("foo", policy, store, listener);

        verify(listener, times(0)).onResponse(any(AcknowledgedResponse.class));
        verify(listener, times(1)).onFailure(thrown);
    }

    public void testGetEnricyPolicyAction_emptyChecks() {
        {
            ResourceNotFoundException exc = expectThrows(ResourceNotFoundException.class,
                () -> TransportPutEnrichPolicyAction.putPolicy(null, null, null, null));

            assertThat(exc.getMessage(), equalTo("name is missing"));
        }
        {
            ResourceNotFoundException exc = expectThrows(ResourceNotFoundException.class,
                () -> TransportPutEnrichPolicyAction.putPolicy("", null, null, null));

            assertThat(exc.getMessage(), equalTo("name is missing"));
        }
    }
}
