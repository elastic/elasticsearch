/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.security.TokenSSLBootstrapCheck;

public class TokenSSLBootsrapCheckTests extends ESTestCase {

    public void testTokenSSLBootstrapCheck() {
        Settings settings = Settings.EMPTY;
        assertFalse(new TokenSSLBootstrapCheck(settings).check());

        settings = Settings.builder()
                .put(NetworkModule.HTTP_ENABLED.getKey(), false)
                .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true).build();
        assertFalse(new TokenSSLBootstrapCheck(settings).check());

        settings = Settings.builder().put(XPackSettings.HTTP_SSL_ENABLED.getKey(), true).build();
        assertFalse(new TokenSSLBootstrapCheck(settings).check());

        // XPackSettings.HTTP_SSL_ENABLED default false
        settings = Settings.builder().put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true).build();
        assertTrue(new TokenSSLBootstrapCheck(settings).check());

        settings = Settings.builder()
                .put(XPackSettings.HTTP_SSL_ENABLED.getKey(), false)
                .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true).build();
        assertTrue(new TokenSSLBootstrapCheck(settings).check());

        settings = Settings.builder()
                .put(XPackSettings.HTTP_SSL_ENABLED.getKey(), false)
                .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true)
                .put(NetworkModule.HTTP_ENABLED.getKey(), false).build();
        assertFalse(new TokenSSLBootstrapCheck(settings).check());
    }
}
