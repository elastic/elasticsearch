/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CompositeServiceAccountsTokenStoreTests extends ESTestCase {

    public void testAuthenticate() {
        final ServiceAccountToken token = mock(ServiceAccountToken.class);

        final ServiceAccountsTokenStore store1 = mock(ServiceAccountsTokenStore.class);
        final ServiceAccountsTokenStore store2 = mock(ServiceAccountsTokenStore.class);
        final ServiceAccountsTokenStore store3 = mock(ServiceAccountsTokenStore.class);

        final boolean store1Success = randomBoolean();
        final boolean store2Success = randomBoolean();
        final boolean store3Success = randomBoolean();

        when(store1.authenticate(token)).thenReturn(store1Success);
        when(store2.authenticate(token)).thenReturn(store2Success);
        when(store3.authenticate(token)).thenReturn(store3Success);

        final ServiceAccountsTokenStore.CompositeServiceAccountsTokenStore compositeStore =
            new ServiceAccountsTokenStore.CompositeServiceAccountsTokenStore(List.of(store1, store2, store3));

        if (store1Success || store2Success || store3Success) {
            assertThat(compositeStore.authenticate(token), is(true));
        } else {
            assertThat(compositeStore.authenticate(token), is(false));
        }
    }
}
