/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.AbstractBootstrapCheckTestCase;
import org.elasticsearch.xpack.core.XPackSettings;

public class TokenSSLBootsrapCheckTests extends AbstractBootstrapCheckTestCase {

    public void testTokenSSLBootstrapCheck() {
        Settings settings = Settings.EMPTY;

        assertTrue(new TokenSSLBootstrapCheck().check(createTestContext(settings, null)).isSuccess());

        settings = Settings.builder().put(XPackSettings.HTTP_SSL_ENABLED.getKey(), true).build();
        assertTrue(new TokenSSLBootstrapCheck().check(createTestContext(settings, null)).isSuccess());

        // XPackSettings.HTTP_SSL_ENABLED default false
        settings = Settings.builder().put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true).build();
        assertTrue(new TokenSSLBootstrapCheck().check(createTestContext(settings, null)).isFailure());

        settings = Settings.builder()
                .put(XPackSettings.HTTP_SSL_ENABLED.getKey(), true)
                .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true).build();
        assertTrue(new TokenSSLBootstrapCheck().check(createTestContext(settings, null)).isSuccess());
    }

}
