/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.logstashbridge.common;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.logstashbridge.StableBridgeAPI;

/**
 * A {@link StableBridgeAPI} for {@link Settings.Builder}.
 */
public interface SettingsBuilderBridge extends StableBridgeAPI<Settings.Builder> {

    SettingsBuilderBridge put(String key, String value);

    SettingsBridge build();

    static SettingsBuilderBridge fromInternal(final Settings.Builder builder) {
        return new ProxyInternal(builder);
    }

    /**
     * An implementation of {@link SettingsBuilderBridge} that proxies calls to
     * an internal {@link Settings.Builder} instance.
     *
     * @see StableBridgeAPI.ProxyInternal
     */
    final class ProxyInternal extends StableBridgeAPI.ProxyInternal<Settings.Builder> implements SettingsBuilderBridge {
        ProxyInternal(Settings.Builder internalDelegate) {
            super(internalDelegate);
        }

        @Override
        public SettingsBuilderBridge put(String key, String value) {
            internalDelegate.put(key, value);
            return this;
        }

        @Override
        public SettingsBridge build() {
            final Settings delegate = internalDelegate.build();
            return SettingsBridge.fromInternal(delegate);
        }
    }
}
