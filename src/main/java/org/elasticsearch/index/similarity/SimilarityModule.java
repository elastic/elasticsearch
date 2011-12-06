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

package org.elasticsearch.index.similarity;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Scopes;
import org.elasticsearch.common.inject.assistedinject.FactoryProvider;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;

/**
 *
 */
public class SimilarityModule extends AbstractModule {

    private final Settings settings;

    public SimilarityModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    protected void configure() {
        MapBinder<String, SimilarityProviderFactory> similarityBinder
                = MapBinder.newMapBinder(binder(), String.class, SimilarityProviderFactory.class);

        Map<String, Settings> similarityProvidersSettings = settings.getGroups("index.similarity");
        for (Map.Entry<String, Settings> entry : similarityProvidersSettings.entrySet()) {
            String name = entry.getKey();
            Settings settings = entry.getValue();

            Class<? extends SimilarityProvider> type = settings.getAsClass("type", null, "org.elasticsearch.index.similarity.", "SimilarityProvider");
            if (type == null) {
                throw new IllegalArgumentException("Similarity [" + name + "] must have a type associated with it");
            }
            similarityBinder.addBinding(name).toProvider(FactoryProvider.newFactory(SimilarityProviderFactory.class, type)).in(Scopes.SINGLETON);
        }

        bind(SimilarityService.class).in(Scopes.SINGLETON);
    }
}
