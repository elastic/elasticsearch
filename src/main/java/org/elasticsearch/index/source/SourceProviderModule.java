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

package org.elasticsearch.index.source;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Scopes;
import org.elasticsearch.common.inject.assistedinject.FactoryProvider;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.settings.NoClassSettingsException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.source.field.DefaultSourceProviderParser;

import java.util.LinkedList;
import java.util.Map;

/**
 *
 */
public class SourceProviderModule extends AbstractModule {

    public static class SourceProviderBinderProcessor {

        public void processSourceProviders(SourceProviderBindings sourceProviderBindings) {

        }

        public static class SourceProviderBindings {
            private final Map<String, Class<? extends SourceProviderParser>> sourceProviders = Maps.newHashMap();

            public SourceProviderBindings() {
            }

            public void processSourceProvider(String name, Class<? extends SourceProviderParser> sourceProviderParser) {
                sourceProviders.put(name, sourceProviderParser);
            }
        }
    }

    private final Settings settings;

    private final LinkedList<SourceProviderBinderProcessor> processors = Lists.newLinkedList();

    public SourceProviderModule(Settings settings) {
        this.settings = settings;
        processors.add(new DefaultProcessor());
    }

    @Override
    protected void configure() {
        bind(SourceProviderService.class).asEagerSingleton();
        MapBinder<String, SourceProviderParserFactory> providerBinder
                = MapBinder.newMapBinder(binder(), String.class, SourceProviderParserFactory.class);

        // initial default bindings
        SourceProviderBinderProcessor.SourceProviderBindings sourceProviderBindings = new SourceProviderBinderProcessor.SourceProviderBindings();
        for (SourceProviderBinderProcessor processor : processors) {
            processor.processSourceProviders(sourceProviderBindings);
        }

        Map<String, Settings> sourceProviderSettings = settings.getGroups(SourceProviderService.Defaults.SOURCE_PREFIX);
        for (Map.Entry<String, Settings> entry : sourceProviderSettings.entrySet()) {
            String providerName = entry.getKey();
            Settings providerSettings = entry.getValue();
            Class<? extends SourceProviderParser> type = null;
            try {
                type = providerSettings.getAsClass("type", null, "org.elasticsearch.index.source.", "SourceProviderParser");
            } catch (NoClassSettingsException e) {
                // nothing found, see if its in bindings as a binding name
                if (providerSettings.get("type") != null) {
                    type = sourceProviderBindings.sourceProviders.get(Strings.toUnderscoreCase(providerSettings.get("type")));
                    if (type == null) {
                        type = sourceProviderBindings.sourceProviders.get(Strings.toCamelCase(providerSettings.get("type")));
                    }
                }
            }
            if (type == null) {
                throw new IllegalArgumentException("Source Provider [" + providerName + "] must be provided with a type");
            }
            providerBinder.addBinding(providerName).toProvider(FactoryProvider.newFactory(SourceProviderParserFactory.class, type)).in(Scopes.SINGLETON);
        }

        // go over the source providers in the bindings and register the ones that are not configured
        for (Map.Entry<String, Class<? extends SourceProviderParser>> entry : sourceProviderBindings.sourceProviders.entrySet()) {
            String providerName = entry.getKey();
            Class<? extends SourceProviderParser> type = entry.getValue();
            // we don't want to re-register one that already exists
            if (sourceProviderSettings.containsKey(providerName)) {
                continue;
            }
            // check, if it requires settings, then don't register it, we know default has no settings...
            if (type.getAnnotation(SourceProviderSettingsRequired.class) != null) {
                continue;
            }
            providerBinder.addBinding(providerName).toProvider(FactoryProvider.newFactory(SourceProviderParserFactory.class, type)).in(Scopes.SINGLETON);
        }

    }

    public SourceProviderModule addProcessor(SourceProviderBinderProcessor processor) {
        processors.addFirst(processor);
        return this;
    }

    private static class DefaultProcessor extends SourceProviderBinderProcessor {

        @Override public void processSourceProviders(SourceProviderBindings sourceProviderBindings) {
            sourceProviderBindings.processSourceProvider("default", DefaultSourceProviderParser.class);
        }
    }
}
