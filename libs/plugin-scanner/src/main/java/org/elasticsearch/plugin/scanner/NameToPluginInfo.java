/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.scanner;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class NameToPluginInfo {

    // plugin name to NamedPluginInfo
    private Map<String, PluginInfo> namedPluginInfoMap = new HashMap<>();

    public void put(String name, PluginInfo pluginInfo) {
        namedPluginInfoMap.put(name, pluginInfo);
    }

    public void putAll(Map<String, PluginInfo> namedPluginInfoMap) {
        this.namedPluginInfoMap.putAll(namedPluginInfoMap);
    }

    public void put(NameToPluginInfo nameToPluginInfo) {
        this.namedPluginInfoMap.putAll(nameToPluginInfo.namedPluginInfoMap);
    }

    public PluginInfo getForPluginName(String pluginName) {
        return namedPluginInfoMap.get(pluginName);
    }

    @Override
    public String toString() {
        return "NameToPluginInfo{" + "namedPluginInfoMap=" + namedPluginInfoMap + '}';
    }

    public NameToPluginInfo with(String name, PluginInfo pluginInfo) {
        put(name, pluginInfo);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NameToPluginInfo that = (NameToPluginInfo) o;
        return Objects.equals(namedPluginInfoMap, that.namedPluginInfoMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namedPluginInfoMap);
    }
}
