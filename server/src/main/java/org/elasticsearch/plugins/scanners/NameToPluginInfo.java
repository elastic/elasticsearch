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

public class NameToPluginInfo {

    // plugin name to NamedPluginInfo
    private Map<String, NamedPluginInfo> namedPluginInfoMap = new HashMap<>();

    public void put(String name, NamedPluginInfo namedPluginInfo) {
        namedPluginInfoMap.put(name, namedPluginInfo);
    }

    public NamedPluginInfo getForPluginName(String pluginName) {
        return namedPluginInfoMap.get(pluginName);
    }
}
