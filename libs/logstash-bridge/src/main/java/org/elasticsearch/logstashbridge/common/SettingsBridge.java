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
public class SettingsBridge extends StableBridgeAPI.ProxyInternal<Settings> {

    public static SettingsBridge fromInternal(final Settings delegate) {
        return new SettingsBridge(delegate);
    }

    public static Builder builder() {
        return Builder.fromInternal(Settings.builder());
    }

    public SettingsBridge(final Settings delegate) {
        super(delegate);
    }

    @Override
    public Settings toInternal() {
        return this.internalDelegate;
    }

    /**
     * An external bridge for {@link Settings.Builder} that proxies calls to a real {@link Settings.Builder}
     */
    public static class Builder extends StableBridgeAPI.ProxyInternal<Settings.Builder> {
        static Builder fromInternal(final Settings.Builder delegate) {
            return new Builder(delegate);
        }

        private Builder(final Settings.Builder delegate) {
            super(delegate);
        }

        public Builder put(final String key, final String value) {
            this.internalDelegate.put(key, value);
            return this;
        }

        public SettingsBridge build() {
            return new SettingsBridge(this.internalDelegate.build());
        }
    }
}
