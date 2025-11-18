/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.Build;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.inference.InferenceCommandConfig;

import java.util.Locale;
import java.util.Map;

class EsqlConfig {

    private final Map<String, InferenceCommandConfig> inferenceCommandConfigs;

    // versioning information
    boolean devVersion = Build.current().isSnapshot();

    public boolean isDevVersion() {
        return devVersion;
    }

    boolean isReleaseVersion() {
        return isDevVersion() == false;
    }

    public void setDevVersion(boolean dev) {
        this.devVersion = dev;
    }

    EsqlConfig(Settings settings) {
        inferenceCommandConfigs = InferenceCommandConfig.fromSettings(settings);
    }

    public InferenceCommandConfig inferenceCommandConfig(String commandName) {
        InferenceCommandConfig config = inferenceCommandConfigs.get(commandName.toLowerCase(Locale.ROOT));

        if (config == null) {
            throw new IllegalArgumentException("Unknown inference command: " + commandName);
        }

        return config;
    }
}
