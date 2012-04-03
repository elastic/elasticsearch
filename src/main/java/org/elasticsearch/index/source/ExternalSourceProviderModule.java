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

import java.util.LinkedList;
import java.util.Map;

/**
 *
 */
public class ExternalSourceProviderModule extends AbstractModule {

    public static class ExternalSourceProviderBinderProcessor {

        public void processSourceProviders(ExternalSourceProviderBindings externalSourceProviderBindings) {

        }

        public static class ExternalSourceProviderBindings {
            private final Map<String, Class<? extends ExternalSourceProvider>> sourceProviders = Maps.newHashMap();

            public ExternalSourceProviderBindings() {
            }

            public void processExternalSourceProvider(String name, Class<? extends ExternalSourceProvider> sourceProvider) {
                sourceProviders.put(name, sourceProvider);
            }
        }
    }

    private final Settings settings;

    private final LinkedList<ExternalSourceProviderBinderProcessor> processorExternals = Lists.newLinkedList();

    public ExternalSourceProviderModule(Settings settings) {
        this.settings = settings;
        processorExternals.add(new DefaultProcessorExternal());
    }

    @Override
    protected void configure() {
        bind(ExternalSourceProviderService.class).asEagerSingleton();
        MapBinder<String, ExternalSourceProviderFactory> providerBinder
                = MapBinder.newMapBinder(binder(), String.class, ExternalSourceProviderFactory.class);

        // initial default bindings
        ExternalSourceProviderBinderProcessor.ExternalSourceProviderBindings externalSourceProviderBindings = new ExternalSourceProviderBinderProcessor.ExternalSourceProviderBindings();
        for (ExternalSourceProviderBinderProcessor processorExternal : processorExternals) {
            processorExternal.processSourceProviders(externalSourceProviderBindings);
        }

        Map<String, Settings> providersSettings = settings.getGroups(ExternalSourceProviderService.Defaults.SOURCE_PROVIDER_PREFIX);
        for (Map.Entry<String, Settings> entry : providersSettings.entrySet()) {
            String providerName = entry.getKey();
            Settings providerSettings = entry.getValue();
            Class<? extends ExternalSourceProvider> type = null;
            try {
                type = providerSettings.getAsClass("type", null, "org.elasticsearch.index.source.", "ExternalSourceProvider");
            } catch (NoClassSettingsException e) {
                // nothing found, see if its in bindings as a binding name
                if (providerSettings.get("type") != null) {
                    type = externalSourceProviderBindings.sourceProviders.get(Strings.toUnderscoreCase(providerSettings.get("type")));
                    if (type == null) {
                        type = externalSourceProviderBindings.sourceProviders.get(Strings.toCamelCase(providerSettings.get("type")));
                    }
                }
            }
            if (type == null) {
                throw new IllegalArgumentException("External Source Provider [" + providerName + "] must be provided with a type");
            }
            providerBinder.addBinding(providerName).toProvider(FactoryProvider.newFactory(ExternalSourceProviderFactory.class, type)).in(Scopes.SINGLETON);
        }

        // go over the source providers in the bindings and register the ones that are not configured
        for (Map.Entry<String, Class<? extends ExternalSourceProvider>> entry : externalSourceProviderBindings.sourceProviders.entrySet()) {
            String providerName = entry.getKey();
            Class<? extends ExternalSourceProvider> type = entry.getValue();
            // we don't want to re-register one that already exists
            if (providersSettings.containsKey(providerName)) {
                continue;
            }
            // check, if it requires settings, then don't register it, we know default has no settings...
            if (type.getAnnotation(ExternalSourceProviderSettingsRequired.class) != null) {
                continue;
            }
            providerBinder.addBinding(providerName).toProvider(FactoryProvider.newFactory(ExternalSourceProviderFactory.class, type)).in(Scopes.SINGLETON);
        }

    }

    public ExternalSourceProviderModule addProcessor(ExternalSourceProviderBinderProcessor processorExternal) {
        processorExternals.addFirst(processorExternal);
        return this;
    }

    private static class DefaultProcessorExternal extends ExternalSourceProviderBinderProcessor {

        @Override public void processSourceProviders(ExternalSourceProviderBindings externalSourceProviderBindings) {
              // No built-in providers for now
              // externalSourceProviderBindings.processExternalSourceProvider("default", DefaultSourceFilterParser.class);
        }
    }
}
