/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasource.azure;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class AzureCredentialIdentityTests extends ESTestCase {

    public void testSameSettingsShareIdentity() {
        AzureConfiguration a = AzureConfiguration.fromFields(null, "account", "key-one", null, "http://azure:1");
        AzureConfiguration b = AzureConfiguration.fromFields(null, "account", "key-one", null, "http://azure:1");
        assertThat(AzureCredentialIdentity.of(a), equalTo(AzureCredentialIdentity.of(b)));
    }

    public void testDifferentKeysDoNotShareIdentity() {
        AzureConfiguration a = AzureConfiguration.fromFields(null, "account", "key-one", null, "http://azure:1");
        AzureConfiguration b = AzureConfiguration.fromFields(null, "account", "key-two", null, "http://azure:1");
        assertThat(AzureCredentialIdentity.of(a), not(equalTo(AzureCredentialIdentity.of(b))));
    }

    public void testDifferentSasTokensDoNotShareIdentity() {
        AzureConfiguration a = AzureConfiguration.fromFields(null, "account", null, "sas-one", "http://azure:1");
        AzureConfiguration b = AzureConfiguration.fromFields(null, "account", null, "sas-two", "http://azure:1");
        assertThat(AzureCredentialIdentity.of(a), not(equalTo(AzureCredentialIdentity.of(b))));
    }

    public void testDifferentEndpointsDoNotShareIdentity() {
        AzureConfiguration a = AzureConfiguration.fromFields(null, "account", "key-one", null, "http://azure:1");
        AzureConfiguration b = AzureConfiguration.fromFields(null, "account", "key-one", null, "http://azure:2");
        assertThat(AzureCredentialIdentity.of(a), not(equalTo(AzureCredentialIdentity.of(b))));
    }

    public void testSecretIsNotExposed() {
        AzureConfiguration config = AzureConfiguration.fromFields(null, "account", "super-secret-key", null, "http://azure:1");
        assertThat(AzureCredentialIdentity.of(config).toString(), not(containsString("super-secret-key")));
    }
}
