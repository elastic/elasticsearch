/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.common.settings.Settings;

import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_COMPRESSION;
import static org.elasticsearch.xpack.core.XPackSettings.HTTP_SSL_ENABLED;

public final class SecurityHttpSettings {

    private SecurityHttpSettings() {}

    public static void overrideSettings(Settings.Builder settingsBuilder, Settings settings) {
        if (HTTP_SSL_ENABLED.get(settings) && SETTING_HTTP_COMPRESSION.exists(settings) == false) {
            settingsBuilder.put(SETTING_HTTP_COMPRESSION.getKey(), false);
        }
    }
}
