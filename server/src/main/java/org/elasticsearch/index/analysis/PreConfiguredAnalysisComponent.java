/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.analysis.PreBuiltCacheFactory;

import java.io.IOException;

/**
 * Shared implementation for pre-configured analysis components.
 */
public abstract class PreConfiguredAnalysisComponent<T> implements AnalysisModule.AnalysisProvider<T> {
    protected final String name;
    protected final PreBuiltCacheFactory.PreBuiltCache<T> cache;

    protected PreConfiguredAnalysisComponent(String name, PreBuiltCacheFactory.CachingStrategy cache) {
        this.name = name;
        this.cache = PreBuiltCacheFactory.getCache(cache);
    }

    protected PreConfiguredAnalysisComponent(String name, PreBuiltCacheFactory.PreBuiltCache<T> cache) {
        this.name = name;
        this.cache = cache;
    }

    @Override
    public T get(IndexSettings indexSettings, Environment environment, String name, Settings settings) throws IOException {
        IndexVersion versionCreated = indexSettings.getIndexVersionCreated();
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

    protected abstract T create(IndexVersion version);
}
