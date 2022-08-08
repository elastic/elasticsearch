/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.scanners;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class NamedPlugins extends AbstractMap<String, NamedPluginInfo> {

     Map<String, NamedPluginInfo> namedPluginInfoMap = new HashMap<>();
    @Override
    public Set<Entry<String, NamedPluginInfo>> entrySet() {
        return namedPluginInfoMap.entrySet();
    }

    @Override
    public NamedPluginInfo put(String key, NamedPluginInfo value) {
        return namedPluginInfoMap.put(key, value);
    }
}
