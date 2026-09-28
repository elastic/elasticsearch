/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasource.gcs;

import org.elasticsearch.test.ESTestCase;

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

    public void testSecretIsNotExposed() {
        String secret = "{\"private_key\":\"super-secret-material\"}";
        GcsConfiguration config = GcsConfiguration.fromFields(secret, "project", "http://gcs:1");
        assertThat(GcsCredentialIdentity.of(config).toString(), not(containsString("super-secret-material")));
    }
}
