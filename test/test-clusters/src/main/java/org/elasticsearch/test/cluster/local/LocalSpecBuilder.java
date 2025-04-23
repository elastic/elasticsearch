/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster.local;

import org.elasticsearch.test.cluster.EnvironmentProvider;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.SettingsProvider;
import org.elasticsearch.test.cluster.SystemPropertyProvider;
import org.elasticsearch.test.cluster.local.LocalClusterSpec.LocalNodeSpec;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.cluster.util.resource.Resource;

import java.nio.file.Path;
import java.util.function.Consumer;
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
     * Ensure module is installed into the distribution when using the {@link DistributionType#INTEG_TEST} distribution. This is ignored
     * when the {@link DistributionType#DEFAULT} is being used.
     */
    T module(String moduleName, Consumer<? super PluginInstallSpec> config);

    /**
     * Ensure plugin is installed into the distribution.
     */
    T plugin(String pluginName);

    /**
     * Ensure plugin is installed into the distribution.
     */
    T plugin(String pluginName, Consumer<? super PluginInstallSpec> config);

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
     * Sets the version of Elasticsearch. Defaults to {@link Version#CURRENT}.
     */
    T version(Version version);

    /**
     * Sets the version of Elasticsearch. Defaults to {@link Version#CURRENT}.
     */
    T version(String version);

    /**
     * Adds a system property to node JVM arguments.
     */
    T systemProperty(String property, String value);

    /**
     * Adds a system property to node JVM arguments computed by the given supplier
     */
    T systemProperty(String property, Supplier<String> supplier);

    /**
     * Adds a system property to node JVM arguments computed by the given supplier
     * when the given predicate evaluates to {@code true}.
     */
    T systemProperty(String setting, Supplier<String> value, Predicate<LocalNodeSpec> predicate);

    /**
     * Register a {@link SystemPropertyProvider}.
     */
    T systemProperties(SystemPropertyProvider systemPropertyProvider);

    /**
     * Adds an additional command line argument to node JVM arguments.
     */
    T jvmArg(String arg);

    /**
     * Register a supplier to provide the config directory. The default config directory
     * is used when the supplier is null or the return value of the supplier is null.
     */
    T withConfigDir(Supplier<Path> configDirSupplier);
}
