/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins;

import org.elasticsearch.jdk.ModuleQualifiedExportsService;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public class TestPluginsLoader extends PluginsLoader {

    public TestPluginsLoader(Path modulesDirectory, Path pluginsDirectory) {
        super(modulesDirectory, pluginsDirectory);
    }

    public TestPluginsLoader() {
        // do nothing, plugins must be loaded somewhere else
    }

    @Override
    protected void addServerExportsService(Map<String, List<ModuleQualifiedExportsService>> qualifiedExports) {
        // tests don't run modular
    }
}
