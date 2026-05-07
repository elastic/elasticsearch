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

    public void testDefault() {
        var extension = new CloudCredentialsExtension.Default();

        assertThat(extension.internalCloudApiKeyService(), notNullValue());
        assertThat(extension.internalCloudApiKeyService(), instanceOf(InternalCloudApiKeyService.Default.class));
        assertThat(extension.credentialManager(), notNullValue());
        assertThat(extension.credentialManager(), instanceOf(CloudCredentialManager.Default.class));
    }

    public void testGetInstanceDefaultsToDefaultExtension() {
        var instance = CloudCredentialsExtension.getInstance();
        assertThat(instance, notNullValue());
    }
}
