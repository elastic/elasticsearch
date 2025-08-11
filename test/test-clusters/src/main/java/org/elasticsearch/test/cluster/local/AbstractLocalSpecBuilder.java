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
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.cluster.util.resource.Resource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public abstract class AbstractLocalSpecBuilder<T extends LocalSpecBuilder<?>> implements LocalSpecBuilder<T> {
    private final AbstractLocalSpecBuilder<?> parent;
    private final List<SettingsProvider> settingsProviders = new ArrayList<>();
    private final Map<String, String> settings = new HashMap<>();
    private final List<EnvironmentProvider> environmentProviders = new ArrayList<>();
    private final Map<String, String> environment = new HashMap<>();
    private final Set<String> modules = new HashSet<>();
    private final Set<String> plugins = new HashSet<>();
    private final Set<FeatureFlag> features = new HashSet<>();
    private final List<SettingsProvider> keystoreProviders = new ArrayList<>();
    private final Map<String, String> keystoreSettings = new HashMap<>();
    private final Map<String, Resource> keystoreFiles = new HashMap<>();
    private final Map<String, Resource> extraConfigFiles = new HashMap<>();
    private final Map<String, String> systemProperties = new HashMap<>();
    private DistributionType distributionType;
    private Version version;
    private String keystorePassword;

    protected AbstractLocalSpecBuilder(AbstractLocalSpecBuilder<?> parent) {
        this.parent = parent;
    }

    @Override
    public T settings(SettingsProvider settingsProvider) {
        this.settingsProviders.add(settingsProvider);
        return cast(this);
    }

    List<SettingsProvider> getSettingsProviders() {
        return inherit(() -> parent.getSettingsProviders(), settingsProviders);
    }

    @Override
    public T setting(String setting, String value) {
        this.settings.put(setting, value);
        return cast(this);
    }

    @Override
    public T setting(String setting, Supplier<String> value) {
        this.settingsProviders.add(s -> Map.of(setting, value.get()));
        return cast(this);
    }

    Map<String, String> getSettings() {
        return inherit(() -> parent.getSettings(), settings);
    }

    @Override
    public T environment(EnvironmentProvider environmentProvider) {
        this.environmentProviders.add(environmentProvider);
        return cast(this);
    }

    List<EnvironmentProvider> getEnvironmentProviders() {
        return inherit(() -> parent.getEnvironmentProviders(), environmentProviders);

    }

    @Override
    public T environment(String key, String value) {
        this.environment.put(key, value);
        return cast(this);
    }

    Map<String, String> getEnvironment() {
        return inherit(() -> parent.getEnvironment(), environment);
    }

    @Override
    public T distribution(DistributionType type) {
        this.distributionType = type;
        return cast(this);
    }

    DistributionType getDistributionType() {
        return inherit(() -> parent.getDistributionType(), distributionType);
    }

    @Override
    public T module(String moduleName) {
        this.modules.add(moduleName);
        return cast(this);
    }

    Set<String> getModules() {
        return inherit(() -> parent.getModules(), modules);
    }

    @Override
    public T plugin(String pluginName) {
        this.plugins.add(pluginName);
        return cast(this);
    }

    Set<String> getPlugins() {
        return inherit(() -> parent.getPlugins(), plugins);
    }

    @Override
    public T feature(FeatureFlag feature) {
        this.features.add(feature);
        return cast(this);
    }

    Set<FeatureFlag> getFeatures() {
        return inherit(() -> parent.getFeatures(), features);
    }

    @Override
    public T keystore(String key, String value) {
        this.keystoreSettings.put(key, value);
        return cast(this);
    }

    public Map<String, String> getKeystoreSettings() {
        return inherit(() -> parent.getKeystoreSettings(), keystoreSettings);
    }

    @Override
    public T keystore(String key, Resource file) {
        this.keystoreFiles.put(key, file);
        return cast(this);
    }

    public Map<String, Resource> getKeystoreFiles() {
        return inherit(() -> parent.getKeystoreFiles(), keystoreFiles);
    }

    @Override
    public T keystore(String key, Supplier<String> supplier) {
        this.keystoreProviders.add(s -> Map.of(key, supplier.get()));
        return cast(this);
    }

    public List<SettingsProvider> getKeystoreProviders() {
        return inherit(() -> parent.getKeystoreProviders(), keystoreProviders);
    }

    @Override
    public T configFile(String fileName, Resource configFile) {
        this.extraConfigFiles.put(fileName, configFile);
        return cast(this);
    }

    public Map<String, Resource> getExtraConfigFiles() {
        return inherit(() -> parent.getExtraConfigFiles(), extraConfigFiles);
    }

    @Override
    public T systemProperty(String property, String value) {
        this.systemProperties.put(property, value);
        return cast(this);
    }

    public Map<String, String> getSystemProperties() {
        return inherit(() -> parent.getSystemProperties(), systemProperties);
    }

    @Override
    public T keystorePassword(String password) {
        this.keystorePassword = password;
        return cast(this);
    }

    public String getKeystorePassword() {
        return inherit(() -> parent.getKeystorePassword(), keystorePassword);
    }

    @Override
    public T version(Version version) {
        this.version = version;
        return cast(this);
    }

    public Version getVersion() {
        return inherit(() -> parent.getVersion(), version);
    }

    private <T> List<T> inherit(Supplier<List<T>> parent, List<T> child) {
        List<T> combinedList = new ArrayList<>();
        if (this.parent != null) {
            combinedList.addAll(parent.get());
        }
        combinedList.addAll(child);
        return combinedList;
    }

    private <T> Set<T> inherit(Supplier<Set<T>> parent, Set<T> child) {
        Set<T> combinedSet = new HashSet<>();
        if (this.parent != null) {
            combinedSet.addAll(parent.get());
        }
        combinedSet.addAll(child);
        return combinedSet;
    }

    private <K, V> Map<K, V> inherit(Supplier<Map<K, V>> parent, Map<K, V> child) {
        Map<K, V> combinedMap = new HashMap<>();
        if (this.parent != null) {
            combinedMap.putAll(parent.get());
        }
        combinedMap.putAll(child);
        return combinedMap;
    }

    private <T> T inherit(Supplier<T> parent, T child) {
        T value = null;
        if (this.parent != null) {
            value = parent.get();
        }
        return child == null ? value : child;
    }

    @SuppressWarnings("unchecked")
    private static <T> T cast(Object o) {
        return (T) o;
    }
}
