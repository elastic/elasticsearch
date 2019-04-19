/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.enrich.EnrichStore;

import static org.elasticsearch.xpack.enrich.EnrichPolicyTests.randomEnrichPolicy;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportGetEnricyPolicyActionTests extends ESTestCase {

    public void testGetEnricyPolicyAction() {
        EnrichStore store = mock(EnrichStore.class);
        EnrichPolicy policy =  randomEnrichPolicy(XContentType.JSON);
        when(store.getPolicy(anyString())).thenReturn(policy);

        EnrichPolicy foundPolicy = TransportGetEnrichPolicyAction.getPolicy("foo", store);
            assertThat(foundPolicy, equalTo(policy));
    }

    public void testGetEnricyPolicyAction_emptyChecks() {
        EnrichStore store = mock(EnrichStore.class);
        when(store.getPolicy(anyString())).thenReturn(null);
        {
            ResourceNotFoundException exc = expectThrows(ResourceNotFoundException.class,
                () -> TransportGetEnrichPolicyAction.getPolicy("foo", store));

            assertThat(exc.getMessage(), equalTo("policy [foo] is missing"));
        }
        {
            ResourceNotFoundException exc = expectThrows(ResourceNotFoundException.class,
                () -> TransportGetEnrichPolicyAction.getPolicy(null, store));

            assertThat(exc.getMessage(), equalTo("name is missing"));
        }
        {
            ResourceNotFoundException exc = expectThrows(ResourceNotFoundException.class,
                () -> TransportGetEnrichPolicyAction.getPolicy("", store));

            assertThat(exc.getMessage(), equalTo("name is missing"));
        }
    }
}
