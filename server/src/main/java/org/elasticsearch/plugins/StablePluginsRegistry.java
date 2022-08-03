/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.plugin.api.Nameable;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class StablePluginsRegistry {
    record NamedPluginInfo(String name, String className, ClassLoader loader ) {

    }

    class NamedPlugin extends AbstractMap<String, NamedPluginInfo> {

        @Override
        public Set<Entry<String, NamedPluginInfo>> entrySet() {
            return null;
        }
    }

    private Map<Class<? extends Nameable>, NamedPlugin> analysisInterfaceToNameComponentClassnameMap = new HashMap<>();


}
