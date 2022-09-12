/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.scanners;

import org.elasticsearch.plugins.PluginBundle;

import java.util.HashMap;
import java.util.Map;

/**
 * A registry of classes declared by plugins as named components.
 * Named components are classes annotated with @NamedComponent(name) and can be referred later by a name given in this annotation.
 * Named components implement/extend Extensibles (classes/interfaces marked with @Extensible)
 */
public class StablePluginsRegistry {

    /*
    A map of an interface/class name marked (effectively) with @Extensible to NameToPluginInfo map
    effectively means that an interface which extends another interface marked with @Extensible is also extensible
    NameToPluginInfo map is a map of Name to PluginInfo
    i.e.
    org.elasticsearch.plugin.analysis.api.TokenFilterFactory ->
        {"nori" -> {nori, org.elasticserach.plugin.analysis.new_nori.NoriReadingFormFilterFactory, classloaderInstance}
     */
    private final Map<String /*Extensible */, NameToPluginInfo> namedComponents;
    private final NamedComponentReader namedComponentsScanner;

    public StablePluginsRegistry() {
        this(new NamedComponentReader(), new HashMap<>());
    }

    // for testing
    StablePluginsRegistry(NamedComponentReader namedComponentReader, HashMap<String /*Extensible */, NameToPluginInfo> namedComponents) {
        this.namedComponentsScanner = namedComponentReader;
        this.namedComponents = namedComponents;
    }

    public void scanBundleForStablePlugins(PluginBundle bundle, ClassLoader pluginClassLoader) {
        Map<String, NameToPluginInfo> namedComponentsFromPlugin = namedComponentsScanner.findNamedComponents(bundle, pluginClassLoader);
        for (Map.Entry<String, NameToPluginInfo> entry : namedComponentsFromPlugin.entrySet()) {
            namedComponents.compute(entry.getKey(), (k, v) -> v != null ? v.put(entry.getValue()) : entry.getValue());
        }
    }

    // TODO this will be removed. getPluginForName or similar will be created
    public Map<String, NameToPluginInfo> getNamedComponents() {
        return namedComponents;
    }

}
