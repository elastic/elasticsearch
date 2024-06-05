/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.scanners;

import java.util.HashMap;
import java.util.Map;

public record NameToPluginInfo(Map<String, PluginInfo> nameToPluginInfoMap) {

    public NameToPluginInfo() {
        this(new HashMap<>());
    }

    public NameToPluginInfo put(String name, PluginInfo pluginInfo) {
        nameToPluginInfoMap.put(name, pluginInfo);
        return this;
    }

    public void putAll(Map<String, PluginInfo> namedPluginInfoMap) {
        this.nameToPluginInfoMap.putAll(namedPluginInfoMap);
    }

    public NameToPluginInfo put(NameToPluginInfo nameToPluginInfo) {
        putAll(nameToPluginInfo.nameToPluginInfoMap);
        return this;
    }

    public PluginInfo getForPluginName(String pluginName) {
        return nameToPluginInfoMap.get(pluginName);
    }

}
