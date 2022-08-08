/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.scanners;


import org.elasticsearch.plugin.api.NamedComponent;
import org.elasticsearch.plugins.PluginBundle;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.Opcodes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableMap;

public class StablePluginsRegistry {

    private Map<String /*Extensible */, NamedPlugins> namedComponents = new HashMap<>();

    public Map<String /*Extensible */, NamedPlugins> scanBundleForStablePlugins(PluginBundle bundle, ClassLoader pluginClassLoader) {

        NamedComponentScanner namedComponentsScanner = new NamedComponentScanner();

        return namedComponentsScanner.findNamedComponents2(bundle, pluginClassLoader);
    }


    public Map<String, NamedPlugins> getNamedComponents() {
        return namedComponents;
    }

}
