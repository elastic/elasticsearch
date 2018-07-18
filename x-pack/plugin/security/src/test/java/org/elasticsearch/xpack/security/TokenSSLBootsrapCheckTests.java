/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.XPackSettings;

public class TokenSSLBootsrapCheckTests extends ESTestCase {

    public void testTokenSSLBootstrapCheck() {
        Settings settings = Settings.EMPTY;

        assertFalse(new TokenSSLBootstrapCheck().check(new BootstrapContext(settings, null)).isFailure());

        settings = Settings.builder().put(XPackSettings.HTTP_SSL_ENABLED.getKey(), true).build();
        assertFalse(new TokenSSLBootstrapCheck().check(new BootstrapContext(settings, null)).isFailure());

        // XPackSettings.HTTP_SSL_ENABLED default false
        settings = Settings.builder().put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true).build();
        assertTrue(new TokenSSLBootstrapCheck().check(new BootstrapContext(settings, null)).isFailure());

        settings = Settings.builder()
                .put(XPackSettings.HTTP_SSL_ENABLED.getKey(), false)
                .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true).build();
        assertTrue(new TokenSSLBootstrapCheck().check(new BootstrapContext(settings, null)).isFailure());
    }
}
