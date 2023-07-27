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
import org.elasticsearch.core.Strings;
import org.elasticsearch.plugins.PluginBundle;
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

/**
 * Reads named components declared by a plugin in a cache file.
 * Cache file is expected to be present in plugin's lib directory
 * <p>
 * The content of a cache file is a JSON representation of a map where:
 * keys -> name of the extensible interface (a class/interface marked with @Extensible)
 * values -> a map of name to implementation class name
 */
public class NamedComponentReader {

    private Logger logger = LogManager.getLogger(NamedComponentReader.class);
    private static final String NAMED_COMPONENTS_FILE_NAME = "named_components.json";
    /**
     * a registry of known classes marked or indirectly marked (extending marked class) with @Extensible
     */
    private final ExtensiblesRegistry extensiblesRegistry;

    public NamedComponentReader() {
        this(ExtensiblesRegistry.INSTANCE);
    }

    NamedComponentReader(ExtensiblesRegistry extensiblesRegistry) {
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
            if (namedComponent != null) {
                Map<String, NameToPluginInfo> namedComponents = readFromFile(namedComponent, pluginClassLoader);
                logger.debug(() -> Strings.format("Plugin in dir %s declared named components %s.", pluginDir, namedComponents));

                return namedComponents;
            }
            logger.debug(() -> Strings.format("No named component defined in plugin dir %s", pluginDir));
        } catch (IOException e) {
            logger.error("unable to read named components", e);
        }
        return emptyMap();
    }

    private static Path findNamedComponentCacheFile(Path pluginDir) throws IOException {
        try (Stream<Path> list = Files.list(pluginDir)) {
            return list.filter(p -> p.getFileName().toString().equals(NAMED_COMPONENTS_FILE_NAME)).findFirst().orElse(null);
        }
    }

    @SuppressWarnings("unchecked")
    Map<String, NameToPluginInfo> readFromFile(Path namedComponent, ClassLoader pluginClassLoader) throws IOException {
        Map<String, NameToPluginInfo> res = new HashMap<>();

        try (
            var json = new BufferedInputStream(Files.newInputStream(namedComponent));
            var parser = JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)
        ) {
            Map<String, Object> map = parser.map();
            for (Map.Entry<String, Object> fileAsMap : map.entrySet()) {
                String extensibleInterface = fileAsMap.getKey();
                validateExtensible(extensibleInterface);
                Map<String, Object> components = (Map<String, Object>) fileAsMap.getValue();
                for (Map.Entry<String, Object> nameToComponent : components.entrySet()) {
                    String name = nameToComponent.getKey();
                    String value = (String) nameToComponent.getValue();

                    res.computeIfAbsent(extensibleInterface, k -> new NameToPluginInfo())
                        .put(name, new PluginInfo(name, value, pluginClassLoader));
                }
            }
        }
        return res;
    }

    private void validateExtensible(String extensibleInterface) {
        if (extensiblesRegistry.hasExtensible(extensibleInterface) == false) {
            throw new IllegalStateException("Unknown extensible name " + extensibleInterface);
        }
    }
}
