/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.scanners;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.plugins.PluginBundle;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xcontent.XContentType.JSON;

public class NamedComponentScanner {

    private Logger logger = LogManager.getLogger(NamedComponentScanner.class);
    private static final String NAMED_COMPONENTS_FILE_NAME = "named_components.json";
    private final ExtensiblesRegistry extensiblesRegistry;

    public NamedComponentScanner() {
        this.extensiblesRegistry = ExtensiblesRegistry.INSTANCE;
    }

    NamedComponentScanner(ExtensiblesRegistry extensiblesRegistry) {
        this.extensiblesRegistry = extensiblesRegistry;
    }

    public Map<String, NameToPluginInfo> findNamedComponents(PluginBundle bundle, ClassLoader pluginClassLoader) {
        Path pluginDir = bundle.getDir();
        return findNamedComponents(pluginDir, pluginClassLoader);
    }

    // scope for testing
    Map<String, NameToPluginInfo> findNamedComponents(Path pluginDir, ClassLoader pluginClassLoader) {
        try {
            Path namedComponent = findNamedComponentCacheFile(pluginDir);
            return readFromFile(namedComponent, pluginClassLoader);
        } catch (IOException e) {
            logger.error("unable to read named components", e);
            return emptyMap();
        }
    }

    private Path findNamedComponentCacheFile(Path pluginDir) throws IOException {
        try (Stream<Path> list = Files.list(pluginDir)) {
            return list.filter(p -> p.getFileName().equals(NAMED_COMPONENTS_FILE_NAME)).findFirst().get();
        }
    }

    private String pathToClassName(String classWithSlashes) {
        return classWithSlashes.replace('/', '.');
    }

    @SuppressWarnings("unchecked")
    Map<String, NameToPluginInfo> readFromFile(Path namedComponent, ClassLoader pluginClassLoader) throws IOException {
        Map<String, NameToPluginInfo> res = new HashMap<>();

        try (var json = new BufferedInputStream(Files.newInputStream(namedComponent))) {
            try (XContentParser parser = JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
                Map<String, Object> map = parser.map();
                for (Map.Entry<String, Object> fileAsMap : map.entrySet()) {
                    String extensibleInterface = fileAsMap.getKey();

                    Map<String, Object> components = (Map<String, Object>) fileAsMap.getValue();
                    for (Map.Entry<String, Object> nameToComponent : components.entrySet()) {
                        String name = nameToComponent.getKey();
                        String value = (String) nameToComponent.getValue();

                        res.computeIfAbsent(extensibleInterface, k -> new NameToPluginInfo())
                            .put(name, new PluginInfo(name, value, pluginClassLoader));
                    }
                }
            }
        }
        return res;
    }
}
