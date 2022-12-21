/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster.local;

import org.elasticsearch.test.cluster.EnvironmentProvider;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.SettingsProvider;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;

interface LocalSpecBuilder<T extends LocalSpecBuilder<?>> {
    /**
     * Register a {@link SettingsProvider}.
     */
    T settings(SettingsProvider settingsProvider);

    /**
     * Add a new node setting.
     */
    T setting(String setting, String value);

    /**
     * Register a {@link EnvironmentProvider}.
     */
    T environment(EnvironmentProvider environmentProvider);

    /**
     * Add a new node environment variable.
     */
    T environment(String key, String value);

    /**
     * Set the cluster {@link DistributionType}. By default, the {@link DistributionType#INTEG_TEST} distribution is used.
     */
    T distribution(DistributionType type);

    /**
     * Ensure module is installed into the distribution when using the {@link DistributionType#INTEG_TEST} distribution. This is ignored
     * when the {@link DistributionType#DEFAULT} is being used.
     */
    T module(String moduleName);

    /**
     * Ensure plugin is installed into the distribution.
     */
    T plugin(String pluginName);

    /**
     * Require feature to be enabled in the cluster.
     */
    T feature(FeatureFlag feature);
}
