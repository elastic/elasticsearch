/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.indexer;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.Modules;
import org.elasticsearch.common.inject.SpawnModules;
import org.elasticsearch.common.settings.NoClassSettingsException;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;

import static org.elasticsearch.common.Strings.*;

/**
 * @author kimchy (shay.banon)
 */
public class IndexerModule extends AbstractModule implements SpawnModules {

    private IndexerName indexerName;

    private final Settings globalSettings;

    private final Map<String, Object> settings;

    public IndexerModule(IndexerName indexerName, Map<String, Object> settings, Settings globalSettings) {
        this.indexerName = indexerName;
        this.globalSettings = globalSettings;
        this.settings = settings;
    }

    @Override public Iterable<? extends Module> spawnModules() {
        return ImmutableList.of(Modules.createModule(loadTypeModule(indexerName.type(), "org.elasticsearch.indexer.", "IndexerModule"), globalSettings));
    }

    @Override protected void configure() {
        bind(Map.class).annotatedWith(IndexerSettings.class).toInstance(settings);
    }

    private Class<? extends Module> loadTypeModule(String type, String prefixPackage, String suffixClassName) {
        String fullClassName = type;
        try {
            return (Class<? extends Module>) globalSettings.getClassLoader().loadClass(fullClassName);
        } catch (ClassNotFoundException e) {
            fullClassName = prefixPackage + Strings.capitalize(toCamelCase(type)) + suffixClassName;
            try {
                return (Class<? extends Module>) globalSettings.getClassLoader().loadClass(fullClassName);
            } catch (ClassNotFoundException e1) {
                fullClassName = prefixPackage + toCamelCase(type) + "." + Strings.capitalize(toCamelCase(type)) + suffixClassName;
                try {
                    return (Class<? extends Module>) globalSettings.getClassLoader().loadClass(fullClassName);
                } catch (ClassNotFoundException e2) {
                    fullClassName = prefixPackage + toCamelCase(type).toLowerCase() + "." + Strings.capitalize(toCamelCase(type)) + suffixClassName;
                    try {
                        return (Class<? extends Module>) globalSettings.getClassLoader().loadClass(fullClassName);
                    } catch (ClassNotFoundException e3) {
                        throw new NoClassSettingsException("Failed to load class with value [" + type + "]", e);
                    }
                }
            }
        }
    }
}
