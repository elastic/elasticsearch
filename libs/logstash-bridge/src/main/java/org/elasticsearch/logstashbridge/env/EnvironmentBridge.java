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

public class EnvironmentBridge extends StableBridgeAPI.Proxy<Environment> {
    public static EnvironmentBridge wrap(final Environment delegate) {
        return new EnvironmentBridge(delegate);
    }

    public EnvironmentBridge(final SettingsBridge settingsBridge, final Path configPath) {
        this(new Environment(settingsBridge.unwrap(), configPath));
    }

    private EnvironmentBridge(final Environment delegate) {
        super(delegate);
    }

    @Override
    public Environment unwrap() {
        return this.delegate;
    }
}
