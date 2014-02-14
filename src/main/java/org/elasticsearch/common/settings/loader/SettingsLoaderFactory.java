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
 * A settings loader factory automatically trying to identify what type of
 * {@link SettingsLoader} to use.
 *
 *
 */
public final class SettingsLoaderFactory {

    private SettingsLoaderFactory() {

    }

    /**
     * Returns a {@link SettingsLoader} based on the resource name.
     */
    public static SettingsLoader loaderFromResource(String resourceName) {
        if (resourceName.endsWith(".json")) {
            return new JsonSettingsLoader();
        } else if (resourceName.endsWith(".yml") || resourceName.endsWith(".yaml")) {
            return new YamlSettingsLoader();
        } else if (resourceName.endsWith(".properties")) {
            return new PropertiesSettingsLoader();
        } else {
            // lets default to the json one
            return new JsonSettingsLoader();
        }
    }

    /**
     * Returns a {@link SettingsLoader} based on the actual settings source.
     */
    public static SettingsLoader loaderFromSource(String source) {
        if (source.indexOf('{') != -1 && source.indexOf('}') != -1) {
            return new JsonSettingsLoader();
        }
        if (source.indexOf(':') != -1) {
            return new YamlSettingsLoader();
        }
        return new PropertiesSettingsLoader();
    }
}
