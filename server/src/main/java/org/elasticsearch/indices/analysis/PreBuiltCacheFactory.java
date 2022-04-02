/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.indices.analysis;

import org.elasticsearch.Version;
import org.elasticsearch.common.util.Maps;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class PreBuiltCacheFactory {

    /**
     * The strategy of caching the analyzer
     *
     * ONE               Exactly one version is stored. Useful for analyzers which do not store version information
     * LUCENE            Exactly one version for each lucene version is stored. Useful to prevent different analyzers with the same version
     * ELASTICSEARCH     Exactly one version per elasticsearch version is stored. Useful if you change an analyzer between elasticsearch
     *                   releases, when the lucene version does not change
     */
    public enum CachingStrategy {
        ONE,
        LUCENE,
        ELASTICSEARCH
    }

    public interface PreBuiltCache<T> {

        T get(Version version);

        void put(Version version, T t);

        Collection<T> values();
    }

    private PreBuiltCacheFactory() {}

    public static <T> PreBuiltCache<T> getCache(CachingStrategy cachingStrategy) {
        return switch (cachingStrategy) {
            case ONE -> new PreBuiltCacheStrategyOne<>();
            case LUCENE -> new PreBuiltCacheStrategyLucene<>();
            case ELASTICSEARCH -> new PreBuiltCacheStrategyElasticsearch<>();
        };
    }

    /**
     * This is a pretty simple cache, it only contains one version
     */
    private static class PreBuiltCacheStrategyOne<T> implements PreBuiltCache<T> {

        private T model = null;

        @Override
        public T get(Version version) {
            return model;
        }

        @Override
        public void put(Version version, T model) {
            this.model = model;
        }

        @Override
        public Collection<T> values() {
            return model == null ? Collections.emptySet() : Collections.singleton(model);
        }
    }

    /**
     * This cache contains one version for each elasticsearch version object
     */
    private static class PreBuiltCacheStrategyElasticsearch<T> implements PreBuiltCache<T> {

        Map<Version, T> mapModel = Maps.newMapWithExpectedSize(2);

        @Override
        public T get(Version version) {
            return mapModel.get(version);
        }

        @Override
        public void put(Version version, T model) {
            mapModel.put(version, model);
        }

        @Override
        public Collection<T> values() {
            return mapModel.values();
        }
    }

    /**
     * This cache uses the lucene version for caching
     */
    private static class PreBuiltCacheStrategyLucene<T> implements PreBuiltCache<T> {

        private Map<org.apache.lucene.util.Version, T> mapModel = Maps.newMapWithExpectedSize(2);

        @Override
        public T get(Version version) {
            return mapModel.get(version.luceneVersion);
        }

        @Override
        public void put(org.elasticsearch.Version version, T model) {
            mapModel.put(version.luceneVersion, model);
        }

        @Override
        public Collection<T> values() {
            return mapModel.values();
        }
    }
}
