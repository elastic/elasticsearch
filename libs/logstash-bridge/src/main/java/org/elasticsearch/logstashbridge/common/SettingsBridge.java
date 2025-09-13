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
 * A {@link StableBridgeAPI} for {@link Settings}
 */
public interface SettingsBridge extends StableBridgeAPI<Settings> {

    static SettingsBridge fromInternal(final Settings delegate) {
        return new ProxyInternal(delegate);
    }

    static SettingsBuilderBridge builder() {
        return SettingsBuilderBridge.fromInternal(Settings.builder());
    }

    /**
     * An implementation of {@link SettingsBridge} that proxies calls to
     * an internal {@link Settings} instance.
     *
     * @see StableBridgeAPI.ProxyInternal
     */
    final class ProxyInternal extends StableBridgeAPI.ProxyInternal<Settings> implements SettingsBridge {
        ProxyInternal(Settings internalDelegate) {
            super(internalDelegate);
        }
    }
}
