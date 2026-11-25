/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasource.gcs;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StorageIdentityCoverage;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class GcsCredentialIdentityTests extends ESTestCase {

    public void testSameSettingsShareIdentity() {
        GcsConfiguration a = GcsConfiguration.fromFields("{\"key\":\"one\"}", "project", "http://gcs:1");
        GcsConfiguration b = GcsConfiguration.fromFields("{\"key\":\"one\"}", "project", "http://gcs:1");
        assertThat(GcsCredentialIdentity.of(a), equalTo(GcsCredentialIdentity.of(b)));
    }

    public void testDifferentCredentialsDoNotShareIdentity() {
        GcsConfiguration a = GcsConfiguration.fromFields("{\"key\":\"one\"}", "project", "http://gcs:1");
        GcsConfiguration b = GcsConfiguration.fromFields("{\"key\":\"two\"}", "project", "http://gcs:1");
        assertThat(GcsCredentialIdentity.of(a), not(equalTo(GcsCredentialIdentity.of(b))));
    }

    public void testDifferentEndpointsDoNotShareIdentity() {
        GcsConfiguration a = GcsConfiguration.fromFields("{\"key\":\"one\"}", "project", "http://gcs:1");
        GcsConfiguration b = GcsConfiguration.fromFields("{\"key\":\"one\"}", "project", "http://gcs:2");
        assertThat(GcsCredentialIdentity.of(a), not(equalTo(GcsCredentialIdentity.of(b))));
    }

    public void testDifferentProjectIdsDoNotShareIdentity() {
        GcsConfiguration a = GcsConfiguration.fromFields("{\"key\":\"one\"}", "project-a", "http://gcs:1");
        GcsConfiguration b = GcsConfiguration.fromFields("{\"key\":\"one\"}", "project-b", "http://gcs:1");
        assertThat(GcsCredentialIdentity.of(a), not(equalTo(GcsCredentialIdentity.of(b))));
    }

    public void testDifferentTokenUrisDoNotShareIdentity() {
        GcsConfiguration a = GcsConfiguration.fromFields("{\"key\":\"one\"}", "project", "http://gcs:1", "https://oauth/a");
        GcsConfiguration b = GcsConfiguration.fromFields("{\"key\":\"one\"}", "project", "http://gcs:1", "https://oauth/b");
        assertThat(GcsCredentialIdentity.of(a), not(equalTo(GcsCredentialIdentity.of(b))));
    }

    public void testDifferentAccessTokensDoNotShareIdentity() {
        GcsConfiguration a = GcsConfiguration.fromMap(Map.of("access_token", "token-a", "endpoint", "http://gcs:1"));
        GcsConfiguration b = GcsConfiguration.fromMap(Map.of("access_token", "token-b", "endpoint", "http://gcs:1"));
        assertThat(GcsCredentialIdentity.of(a), not(equalTo(GcsCredentialIdentity.of(b))));
        assertThat(GcsCredentialIdentity.of(a).toString(), not(containsString("token-a")));
    }

    public void testDifferentJwtAudiencesDoNotShareIdentity() {
        GcsConfiguration a = federated("aud-a", "sts", null);
        GcsConfiguration b = federated("aud-b", "sts", null);
        assertThat(GcsCredentialIdentity.of(a), not(equalTo(GcsCredentialIdentity.of(b))));
    }

    public void testDifferentStsAudiencesDoNotShareIdentity() {
        GcsConfiguration a = federated(null, "sts-a", null);
        GcsConfiguration b = federated(null, "sts-b", null);
        assertThat(GcsCredentialIdentity.of(a), not(equalTo(GcsCredentialIdentity.of(b))));
    }

    public void testDifferentImpersonationUrlsDoNotShareIdentity() {
        GcsConfiguration a = federated(null, "sts", "https://iam/a");
        GcsConfiguration b = federated(null, "sts", "https://iam/b");
        assertThat(GcsCredentialIdentity.of(a), not(equalTo(GcsCredentialIdentity.of(b))));
    }

    public void testAnonymousAndManagedIdentityDoNotShareIdentity() {
        GcsConfiguration anon = GcsConfiguration.fromMap(Map.of("auth", "anonymous", "endpoint", "http://gcs:1"));
        GcsConfiguration managed = GcsConfiguration.fromMap(Map.of("auth", "managed_identity", "endpoint", "http://gcs:1"));
        assertThat(GcsCredentialIdentity.of(anon), not(equalTo(GcsCredentialIdentity.of(managed))));
    }

    public void testSecretIsNotExposed() {
        String secret = "{\"private_key\":\"super-secret-material\"}";
        GcsConfiguration config = GcsConfiguration.fromFields(secret, "project", "http://gcs:1");
        assertThat(GcsCredentialIdentity.of(config).toString(), not(containsString("super-secret-material")));
    }

    private static GcsConfiguration federated(String jwtAudience, String stsAudience, String impersonationUrl) {
        return GcsConfiguration.fromFields(null, "project", "http://gcs:1", null, null, jwtAudience, stsAudience, impersonationUrl);
    }

    /**
     * The census, derived rather than listed. Every per-field test above names its field, so none of them can fail
     * when this provider GAINS a setting that never reaches the identity — and two data sources differing only in
     * that setting would then share cached bytes. This asks the configuration which settings it declares.
     */
    public void testEverySettingReachesTheIdentity() {
        GcsConfiguration config = GcsConfiguration.fromFields("{\"key\":\"one\"}", "project", "http://gcs:1");
        StorageIdentityCoverage.assertEverySettingReachesTheIdentity(
            config,
            GcsCredentialIdentity.of(config),
            Map.of("auth", "authMode"),
            Map.of()
        );
    }

}
