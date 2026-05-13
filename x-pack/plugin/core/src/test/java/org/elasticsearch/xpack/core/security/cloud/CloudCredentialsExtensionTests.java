/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.cloud;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class CloudCredentialsExtensionTests extends ESTestCase {

    public void testNoop() {
        var extension = new CloudCredentialsExtension.Noop();

        assertThat(extension.internalCloudApiKeyService(), notNullValue());
        assertThat(extension.internalCloudApiKeyService(), instanceOf(InternalCloudApiKeyService.Default.class));
        assertThat(extension.credentialManager(), notNullValue());
        assertThat(extension.credentialManager(), instanceOf(CloudCredentialManager.Noop.class));
    }

    public void testGetInstanceDefaultsToNoopExtension() {
        var instance = CloudCredentialsExtension.getInstance();
        assertThat(instance, notNullValue());
        assertThat(instance, instanceOf(CloudCredentialsExtension.Noop.class));
        assertThat(instance.credentialManager(), instanceOf(CloudCredentialManager.Noop.class));
        assertThat(instance.internalCloudApiKeyService(), instanceOf(InternalCloudApiKeyService.Default.class));
    }

    public void testSetInstanceRejectsNull() {
        expectThrows(NullPointerException.class, () -> CloudCredentialsExtension.setInstance(null));
        // The reference must not have been mutated by a failed call.
        assertThat(CloudCredentialsExtension.getInstance(), instanceOf(CloudCredentialsExtension.Noop.class));
    }
}
