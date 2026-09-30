/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasource.azure;

import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

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

    public void testDifferentAccountsDoNotShareIdentity() {
        AzureConfiguration a = AzureConfiguration.fromFields(null, "account-a", "key-one", null, "http://azure:1");
        AzureConfiguration b = AzureConfiguration.fromFields(null, "account-b", "key-one", null, "http://azure:1");
        assertThat(AzureCredentialIdentity.of(a), not(equalTo(AzureCredentialIdentity.of(b))));
    }

    public void testDifferentConnectionStringsDoNotShareIdentity() {
        AzureConfiguration a = AzureConfiguration.fromFields("conn-a", null, null, null, "http://azure:1");
        AzureConfiguration b = AzureConfiguration.fromFields("conn-b", null, null, null, "http://azure:1");
        assertThat(AzureCredentialIdentity.of(a), not(equalTo(AzureCredentialIdentity.of(b))));
        assertThat(AzureCredentialIdentity.of(a).toString(), not(containsString("conn-a")));
    }

    public void testDifferentTenantIdsDoNotShareIdentity() {
        AzureConfiguration a = federated("tenant-a", "client", null);
        AzureConfiguration b = federated("tenant-b", "client", null);
        assertThat(AzureCredentialIdentity.of(a), not(equalTo(AzureCredentialIdentity.of(b))));
    }

    public void testDifferentClientIdsDoNotShareIdentity() {
        AzureConfiguration a = federated("tenant", "client-a", null);
        AzureConfiguration b = federated("tenant", "client-b", null);
        assertThat(AzureCredentialIdentity.of(a), not(equalTo(AzureCredentialIdentity.of(b))));
    }

    public void testDifferentJwtAudiencesDoNotShareIdentity() {
        AzureConfiguration a = federated("tenant", "client", "aud-a");
        AzureConfiguration b = federated("tenant", "client", "aud-b");
        assertThat(AzureCredentialIdentity.of(a), not(equalTo(AzureCredentialIdentity.of(b))));
    }

    public void testAnonymousAndManagedIdentityDoNotShareIdentity() {
        AzureConfiguration anon = AzureConfiguration.fromFields(null, "account", null, null, "http://azure:1", "anonymous");
        AzureConfiguration managed = AzureConfiguration.fromFields(null, "account", null, null, "http://azure:1", "managed_identity");
        assertThat(AzureCredentialIdentity.of(anon), not(equalTo(AzureCredentialIdentity.of(managed))));
    }

    public void testSecretIsNotExposed() {
        AzureConfiguration config = AzureConfiguration.fromFields(null, "account", "super-secret-key", null, "http://azure:1");
        assertThat(AzureCredentialIdentity.of(config).toString(), not(containsString("super-secret-key")));
    }

    private static AzureConfiguration federated(String tenantId, String clientId, String jwtAudience) {
        Map<String, Object> raw = new HashMap<>();
        raw.put("tenant_id", tenantId);
        raw.put("client_id", clientId);
        raw.put("endpoint", "http://azure:1");
        if (jwtAudience != null) {
            raw.put("jwt_audience", jwtAudience);
        }
        return AzureConfiguration.fromMap(raw);
    }
}
