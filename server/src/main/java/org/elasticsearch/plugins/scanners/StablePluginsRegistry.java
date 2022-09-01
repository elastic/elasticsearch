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

public class StablePluginsRegistry {

    /*
    A map of an interface/class name marked (effectively) with @Extensible to NameToPluginInfo map
    effectively means that an interface which extends another interface marked with @Extensible is also extensible
    NameToPluginInfo map is a map of Name to PluginInfo
    i.e.
    org.elasticsearch.plugin.analysis.api.TokenFilterFactory ->
        {"nori" -> {nori, org.elasticserach.plugin.analysis.new_nori.NoriReadingFormFilterFactory, classloaderInstance}
     */
    private final Map<String /*Extensible */, NameToPluginInfo> namedComponents = new HashMap<>();
    private final NamedComponentScanner namedComponentsScanner = new NamedComponentScanner();

    public void scanBundleForStablePlugins(PluginBundle bundle, ClassLoader pluginClassLoader) {

        Map<String, NameToPluginInfo> namedComponentsFromPlugin = namedComponentsScanner.findNamedComponents(bundle, pluginClassLoader);
        namedComponents.putAll(namedComponentsFromPlugin);
    }

    // TODO this will be removed. getPluginForName or similar wil be created
    public Map<String, NameToPluginInfo> getNamedComponents() {
        return namedComponents;
    }

}
