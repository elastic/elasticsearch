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

public class SettingsBridge extends StableBridgeAPI.Proxy<Settings> {

    public static SettingsBridge wrap(final Settings delegate) {
        return new SettingsBridge(delegate);
    }

    public static Builder builder() {
        return Builder.wrap(Settings.builder());
    }

    public SettingsBridge(final Settings delegate) {
        super(delegate);
    }

    @Override
    public Settings unwrap() {
        return this.delegate;
    }

    public static class Builder extends StableBridgeAPI.Proxy<Settings.Builder> {
        static Builder wrap(final Settings.Builder delegate) {
            return new Builder(delegate);
        }

        private Builder(final Settings.Builder delegate) {
            super(delegate);
        }

        public Builder put(final String key, final String value) {
            this.delegate.put(key, value);
            return this;
        }

        public SettingsBridge build() {
            return new SettingsBridge(this.delegate.build());
        }
    }
}
