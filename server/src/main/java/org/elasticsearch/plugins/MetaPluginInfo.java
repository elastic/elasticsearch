/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugins;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An in-memory representation of the meta plugin descriptor.
 */
public class MetaPluginInfo {
    static final String ES_META_PLUGIN_PROPERTIES = "meta-plugin-descriptor.properties";

    private final String name;
    private final String description;

    /**
     * Construct plugin info.
     *
     * @param name                the name of the plugin
     * @param description         a description of the plugin
     */
    private MetaPluginInfo(String name, String description) {
        this.name = name;
        this.description = description;
    }

    /**
     * @return Whether the provided {@code path} is a meta plugin.
     */
    public static boolean isMetaPlugin(final Path path) {
        return Files.exists(path.resolve(ES_META_PLUGIN_PROPERTIES));
    }

    /**
     * @return Whether the provided {@code path} is a meta properties file.
     */
    public static boolean isPropertiesFile(final Path path) {
        return ES_META_PLUGIN_PROPERTIES.equals(path.getFileName().toString());
    }

    /** reads (and validates) meta plugin metadata descriptor file */

    /**
     * Reads and validates the meta plugin descriptor file.
     *
     * @param path the path to the root directory for the meta plugin
     * @return the meta plugin info
     * @throws IOException if an I/O exception occurred reading the meta plugin descriptor
     */
    public static MetaPluginInfo readFromProperties(final Path path) throws IOException {
        final Path descriptor = path.resolve(ES_META_PLUGIN_PROPERTIES);

        final Map<String, String> propsMap;
        {
            final Properties props = new Properties();
            try (InputStream stream = Files.newInputStream(descriptor)) {
                props.load(stream);
            }
            propsMap = props.stringPropertyNames().stream().collect(Collectors.toMap(Function.identity(), props::getProperty));
        }

        final String name = propsMap.remove("name");
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException(
                    "property [name] is missing for meta plugin in [" + descriptor + "]");
        }
        final String description = propsMap.remove("description");
        if (description == null) {
            throw new IllegalArgumentException(
                    "property [description] is missing for meta plugin [" + name + "]");
        }

        if (propsMap.isEmpty() == false) {
            throw new IllegalArgumentException("Unknown properties in meta plugin descriptor: " + propsMap.keySet());
        }

        return new MetaPluginInfo(name, description);
    }

    /**
     * The name of the meta plugin.
     *
     * @return the meta plugin name
     */
    public String getName() {
        return name;
    }

    /**
     * The description of the meta plugin.
     *
     * @return the meta plugin description
     */
    public String getDescription() {
        return description;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MetaPluginInfo that = (MetaPluginInfo) o;

        if (!name.equals(that.name)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        final StringBuilder information = new StringBuilder()
                .append("- Plugin information:\n")
                .append("Name: ").append(name).append("\n")
                .append("Description: ").append(description);
        return information.toString();
    }

}
