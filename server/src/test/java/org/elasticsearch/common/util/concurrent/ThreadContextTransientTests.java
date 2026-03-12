/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class ThreadContextTransientTests extends ESTestCase {

    public void testSetAndGetSecureStringValue() {
        final String key = randomAlphanumericOfLength(12);
        final ThreadContextTransient<SecureString> tcv = ThreadContextTransient.transientValue(key, SecureString.class);

        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        assertThat(tcv.exists(threadContext), is(false));
        assertThat(tcv.get(threadContext), nullValue());
        expectThrows(IllegalStateException.class, () -> tcv.require(threadContext));

        final SecureString value = new SecureString(randomAlphanumericOfLength(8).toCharArray());
        tcv.set(threadContext, value);
        assertThat(tcv.exists(threadContext), is(true));
        assertThat(tcv.get(threadContext), sameInstance(value));
        assertThat(tcv.require(threadContext), sameInstance(value));

        final SecureString value2 = new SecureString(randomAlphanumericOfLength(10).toCharArray());
        assertThat(tcv.setIfEmpty(threadContext, value2), is(false));
        assertThat(tcv.get(threadContext), sameInstance(value));
        assertThat(tcv.require(threadContext), sameInstance(value));

        try (var restore = threadContext.stashContext()) {
            assertThat(tcv.exists(threadContext), is(false));
            assertThat(tcv.get(threadContext), nullValue());

            assertThat(tcv.setIfEmpty(threadContext, value2), is(true));
            assertThat(tcv.get(threadContext), sameInstance(value2));
            assertThat(tcv.require(threadContext), sameInstance(value2));
        }

        assertThat(tcv.get(threadContext), sameInstance(value));
        assertThat(tcv.require(threadContext), sameInstance(value));
    }

}
