/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.logstashbridge.env;

import org.elasticsearch.env.Environment;
import org.elasticsearch.logstashbridge.StableBridgeAPI;
import org.elasticsearch.logstashbridge.common.SettingsBridge;

import java.nio.file.Path;

/**
 * A {@link StableBridgeAPI} for {@link Environment}
 */
public interface EnvironmentBridge extends StableBridgeAPI<Environment> {
    static EnvironmentBridge fromInternal(final Environment delegate) {
        return new EnvironmentBridge.ProxyInternal(delegate);
    }

    static EnvironmentBridge create(final SettingsBridge bridgedSettings, final Path configPath) {
        return fromInternal(new Environment(bridgedSettings.toInternal(), configPath));
    }

    /**
     * An implementation of {@link EnvironmentBridge} that proxies calls through
     * to an internal {@link Environment}.
     * @see StableBridgeAPI.ProxyInternal
     */
    final class ProxyInternal extends StableBridgeAPI.ProxyInternal<Environment> implements EnvironmentBridge {
        private ProxyInternal(final Environment delegate) {
            super(delegate);
        }

        @Override
        public Environment toInternal() {
            return this.internalDelegate;
        }
    }
}
