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
import org.elasticsearch.test.cluster.local.LocalClusterSpec.LocalNodeSpec;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.cluster.util.resource.Resource;

import java.util.function.Predicate;
import java.util.function.Supplier;

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
     * Add a new node setting computed by the given supplier.
     */
    T setting(String setting, Supplier<String> value);

    /**
     * Add a new node setting computed by the given supplier when the given predicate evaluates to {@code true}.
     */
    T setting(String setting, Supplier<String> value, Predicate<LocalNodeSpec> predicate);

    /**
     * Register a {@link EnvironmentProvider}.
     */
    T environment(EnvironmentProvider environmentProvider);

    /**
     * Add a new node environment variable.
     */
    T environment(String key, String value);

    /**
     * Add a new node environment variable computed by the given supplier.
     */
    T environment(String key, Supplier<String> supplier);

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

    /**
     * Adds a secure setting to the node keystore.
     */
    T keystore(String key, String value);

    /**
     * Adds a secure file to the node keystore.
     */
    T keystore(String key, Resource file);

    /**
     * Add a secure setting computed by the given supplier.
     */
    T keystore(String key, Supplier<String> supplier);

    /**
     * Add a secure setting computed by the given supplier when the given predicate evaluates to {@code true}.
     */
    T keystore(String key, Supplier<String> supplier, Predicate<LocalNodeSpec> predicate);

    /**
     * Register a {@link SettingsProvider} for keystore settings.
     */
    T keystore(SettingsProvider settingsProvider);

    /**
     * Sets the security setting keystore password.
     */
    T keystorePassword(String password);

    /**
     * Adds a file to the node config directory
     */
    T configFile(String fileName, Resource configFile);

    /**
     * Adds a secret to the local secure settings file. This should be used instead of {@link #keystore(String, String)} when file-based
     * secure settings are enabled.
     */
    T secret(String key, String value);

    /**
     * Sets the version of Elasticsearch. Defaults to {@link Version#CURRENT}.
     */
    T version(Version version);

    /**
     * Adds a system property to node JVM arguments.
     */
    T systemProperty(String property, String value);

    /**
     * Adds an additional command line argument to node JVM arguments.
     */
    T jvmArg(String arg);
}
