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

package org.elasticsearch.river;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.Modules;
import org.elasticsearch.common.inject.SpawnModules;
import org.elasticsearch.common.settings.NoClassSettingsException;
import org.elasticsearch.common.settings.Settings;

import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.common.Strings.toCamelCase;

/**
 *
 */
public class RiverModule extends AbstractModule implements SpawnModules {

    private RiverName riverName;

    private final Settings globalSettings;

    private final Map<String, Object> settings;

    private final RiversTypesRegistry typesRegistry;

    public RiverModule(RiverName riverName, Map<String, Object> settings, Settings globalSettings, RiversTypesRegistry typesRegistry) {
        this.riverName = riverName;
        this.globalSettings = globalSettings;
        this.settings = settings;
        this.typesRegistry = typesRegistry;
    }

    @Override
    public Iterable<? extends Module> spawnModules() {
        return ImmutableList.of(Modules.createModule(loadTypeModule(riverName.type(), "org.elasticsearch.river.", "RiverModule"), globalSettings));
    }

    @Override
    protected void configure() {
        bind(RiverSettings.class).toInstance(new RiverSettings(globalSettings, settings));
    }

    private Class<? extends Module> loadTypeModule(String type, String prefixPackage, String suffixClassName) {
        Class<? extends Module> registered = typesRegistry.type(type);
        if (registered != null) {
            return registered;
        }
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
                    fullClassName = prefixPackage + toCamelCase(type).toLowerCase(Locale.ROOT) + "." + Strings.capitalize(toCamelCase(type)) + suffixClassName;
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
