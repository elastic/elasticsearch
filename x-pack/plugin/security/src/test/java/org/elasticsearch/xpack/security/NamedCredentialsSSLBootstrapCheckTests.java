/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.AbstractBootstrapCheckTestCase;

public class NamedCredentialsSSLBootstrapCheckTests extends AbstractBootstrapCheckTestCase {

    public void testFailsWhenExplicitlyEnabledWithoutHttps() {
        Settings settings = Settings.builder().put("xpack.security.named_credentials.enabled", true).build();
        assertTrue(new NamedCredentialsSSLBootstrapCheck().check(createTestContext(settings, Metadata.EMPTY_METADATA)).isFailure());
    }

    public void testPassesWhenHttpsEnabled() {
        Settings settings = Settings.builder()
            .put("xpack.security.http.ssl.enabled", true)
            .put("xpack.security.named_credentials.enabled", true)
            .build();
        assertFalse(new NamedCredentialsSSLBootstrapCheck().check(createTestContext(settings, Metadata.EMPTY_METADATA)).isFailure());
    }

    public void testPassesByDefault() {
        assertFalse(new NamedCredentialsSSLBootstrapCheck().check(createTestContext(Settings.EMPTY, Metadata.EMPTY_METADATA)).isFailure());
    }
}
