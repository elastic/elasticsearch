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

package org.elasticsearch.common.settings.loader;

/**
 * A class holding factory methods for settings loaders that attempts
 * to infer the type of the underlying settings content.
 */
public final class SettingsLoaderFactory {

    private SettingsLoaderFactory() {
    }

    /**
     * Returns a {@link SettingsLoader} based on the source resource
     * name. This factory method assumes that if the resource name ends
     * with ".json" then the content should be parsed as JSON, else if
     * the resource name ends with ".yml" or ".yaml" then the content
     * should be parsed as YAML, else if the resource name ends with
     * ".properties" then the content should be parsed as properties,
     * otherwise default to attempting to parse as JSON. Note that the
     * parsers returned by this method will not accept null-valued
     * keys.
     *
     * @param resourceName The resource name containing the settings
     *                     content.
     * @return A settings loader.
     */
    public static SettingsLoader loaderFromResource(String resourceName) {
        if (resourceName.endsWith(".json")) {
            return new JsonSettingsLoader(false);
        } else if (resourceName.endsWith(".yml") || resourceName.endsWith(".yaml")) {
            return new YamlSettingsLoader(false);
        } else if (resourceName.endsWith(".properties")) {
            return new PropertiesSettingsLoader();
        } else {
            // lets default to the json one
            return new JsonSettingsLoader(false);
        }
    }

    /**
     * Returns a {@link SettingsLoader} based on the source content.
     * This factory method assumes that if the underlying content
     * contains an opening and closing brace ('{' and '}') then the
     * content should be parsed as JSON, else if the underlying content
     * fails this condition but contains a ':' then the content should
     * be parsed as YAML, and otherwise should be parsed as properties.
     * Note that the JSON and YAML parsers returned by this method will
     * accept null-valued keys.
     *
     * @param source The underlying settings content.
     * @return A settings loader.
     */
    public static SettingsLoader loaderFromSource(String source) {
        if (source.indexOf('{') != -1 && source.indexOf('}') != -1) {
            return new JsonSettingsLoader(true);
        }
        if (source.indexOf(':') != -1) {
            return new YamlSettingsLoader(true);
        }
        return new PropertiesSettingsLoader();
    }

}
