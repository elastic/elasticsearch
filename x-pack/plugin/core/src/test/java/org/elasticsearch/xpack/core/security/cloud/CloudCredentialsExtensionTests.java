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
import static org.hamcrest.Matchers.sameInstance;

public class CloudCredentialsExtensionTests extends ESTestCase {

    public void testDefaultExposesNoOpSubSpis() {
        var extension = new CloudCredentialsExtension.Default();

        assertThat(extension.internalCloudApiKeyService(), notNullValue());
        assertThat(extension.internalCloudApiKeyService(), instanceOf(InternalCloudApiKeyService.Default.class));
        assertThat(extension.credentialManager(), notNullValue());
        assertThat(extension.credentialManager(), instanceOf(CloudCredentialManager.Default.class));
    }

    public void testGetInstanceDelegatesToRegisteredInstance() {
        var previous = CloudCredentialsExtension.getInstance();
        var apiKeys = new InternalCloudApiKeyService.Default();
        var manager = new CloudCredentialManager.Default();
        var custom = new CloudCredentialsExtension() {
            @Override
            public InternalCloudApiKeyService internalCloudApiKeyService() {
                return apiKeys;
            }

            @Override
            public CloudCredentialManager credentialManager() {
                return manager;
            }
        };
        try {
            CloudCredentialsExtension.setInstance(custom);
            assertThat(CloudCredentialsExtension.getInstance(), sameInstance(custom));
            assertThat(CloudCredentialsExtension.getInstance().internalCloudApiKeyService(), sameInstance(apiKeys));
            assertThat(CloudCredentialsExtension.getInstance().credentialManager(), sameInstance(manager));
        } finally {
            CloudCredentialsExtension.setInstance(previous);
        }
    }

    public void testGetInstanceDefaultsToDefaultExtension() {
        // The static REFERENCE is initialized to a Default; if any test installed something else and
        // failed to restore, this test will surface that as a regression.
        var instance = CloudCredentialsExtension.getInstance();
        assertThat(instance, notNullValue());
    }
}
