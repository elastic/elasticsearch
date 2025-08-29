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
 * An external bridge for {@link Settings}
 */
public interface SettingsBridge extends StableBridgeAPI<Settings> {

    static SettingsBridge fromInternal(final Settings delegate) {
        return new SettingsBridge.ProxyInternal(delegate);
    }

    static Builder builder() {
        return Builder.fromInternal(Settings.builder());
    }

    class ProxyInternal extends StableBridgeAPI.ProxyInternal<Settings> implements SettingsBridge {
        public ProxyInternal(Settings internalDelegate) {
            super(internalDelegate);
        }
    }

    /**
     * An external bridge for {@link Settings.Builder} that proxies calls to a real {@link Settings.Builder}
     */
    interface Builder extends StableBridgeAPI<Settings.Builder> {

        Builder put(String key, String value);

        SettingsBridge build();

        static Builder fromInternal(final Settings.Builder builder) {
            return new ProxyInternal(builder);
        }

        class ProxyInternal extends StableBridgeAPI.ProxyInternal<Settings.Builder> implements Builder {
            public ProxyInternal(Settings.Builder internalDelegate) {
                super(internalDelegate);
            }

            @Override
            public Builder put(String key, String value) {
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
}
