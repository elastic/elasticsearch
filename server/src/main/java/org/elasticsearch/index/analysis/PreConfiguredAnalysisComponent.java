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

package org.elasticsearch.index.analysis;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.analysis.PreBuiltCacheFactory;

import java.io.IOException;

/**
 * Shared implementation for pre-configured analysis components.
 */
public abstract class PreConfiguredAnalysisComponent<T> implements AnalysisModule.AnalysisProvider<T> {
    private final String name;
    private final PreBuiltCacheFactory.PreBuiltCache<T> cache;

    protected PreConfiguredAnalysisComponent(String name,  PreBuiltCacheFactory.CachingStrategy cache) {
        this.name = name;
        this.cache = PreBuiltCacheFactory.getCache(cache);
    }

    @Override
    public T get(IndexSettings indexSettings, Environment environment, String name, Settings settings) throws IOException {
        Version versionCreated = Version.indexCreated(settings);
        synchronized (this) {
            T factory = cache.get(versionCreated);
            if (factory == null) {
                factory = create(versionCreated);
                cache.put(versionCreated, factory);
            }
            return factory;
        }
    }

    /**
     * The name of the analysis component in the API.
     */
    public String getName() {
        return name;
    }

    protected abstract T create(Version version);
}
