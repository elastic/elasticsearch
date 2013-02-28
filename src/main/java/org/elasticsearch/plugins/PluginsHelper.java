/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.Sets;
import org.elasticsearch.env.Environment;

import java.io.File;
import java.util.Set;

/**
 * Helper class for plugins
 */
public class PluginsHelper {

    /**
     * Build a list of existing site plugins for a given environment
     * @param environment We look into Environment#pluginsFile()
     * @return A Set of existing site plugins
     */
    public static Set<String> sitePlugins(Environment environment) {
        File pluginsFile = environment.pluginsFile();
        Set<String> sitePlugins = Sets.newHashSet();

        if (!pluginsFile.exists() || !pluginsFile.isDirectory()) {
            return sitePlugins;
        }

        for (File pluginFile : pluginsFile.listFiles()) {
            if (new File(pluginFile, "_site").exists()) {
                sitePlugins.add(pluginFile.getName());
            }
        }
        return sitePlugins;
    }

}
