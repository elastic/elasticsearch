/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.cloud;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.cloud.InternalCloudApiKeyService.CloudGrantApiKeyResult;

import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class InternalCloudApiKeyServiceTests extends ESTestCase {

    public void testDefaultGrantFailsListenerWithUnsupportedOperation() {
        var service = new InternalCloudApiKeyService.Default();
        var threadContext = new ThreadContext(org.elasticsearch.common.settings.Settings.EMPTY);
        var credential = new CloudCredential(new SecureString("v".toCharArray()));

        var failure = new AtomicReference<Exception>();
        var success = new AtomicReference<CloudGrantApiKeyResult>();
        ActionListener<CloudGrantApiKeyResult> listener = ActionListener.wrap(success::set, failure::set);

        service.grantCloudAuthentication(threadContext, credential, "transform:test", listener);

        assertNull(success.get());
        assertThat(failure.get(), notNullValue());
        assertThat(failure.get(), instanceOf(UnsupportedOperationException.class));
    }
}
