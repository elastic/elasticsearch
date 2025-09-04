/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.common.settings.Settings;

import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_COMPRESSION;
import static org.elasticsearch.xpack.core.XPackSettings.HTTP_SSL_ENABLED;

public final class SecurityHttpSettings {

    private SecurityHttpSettings() {}

    public static void overrideSettings(Settings.Builder settingsBuilder, Settings settings) {
        // HTTP response compression over TLS risks side-channel vulnerabilities such as BREACH[1] if ES is used in very specific ways. We
        // cannot be sure that ES is not being used in such a manner here, so we disable compression by default when TLS is enabled for the
        // REST layer and rely on the user explicitly setting `http.compression: true` to confirm that they do not have a vulnerable
        // usage pattern.
        //
        // [1] https://www.breachattack.com/
        if (HTTP_SSL_ENABLED.get(settings) && SETTING_HTTP_COMPRESSION.exists(settings) == false) {
            settingsBuilder.put(SETTING_HTTP_COMPRESSION.getKey(), false);
        }
    }
}
