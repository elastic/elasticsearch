/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.indices.analysis;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.IndexVersion;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class PreBuiltCacheFactory {

    /**
     * The strategy of caching the analyzer
     *
     * ONE        Exactly one version is stored. Useful for analyzers which do not store version information
     * LUCENE     Exactly one version for each lucene version is stored. Useful to prevent different analyzers with the same version
     * INDEX      Exactly one version for each index version is stored. Useful if you change an analyzer between index changes,
     *            when the lucene version does not change
     */
    public enum CachingStrategy {
        ONE,
        LUCENE,
        INDEX
    }

    public interface PreBuiltCache<T> {

        T get(IndexVersion version);

        void put(IndexVersion version, T t);

        Collection<T> values();
    }

    private PreBuiltCacheFactory() {}

    public static <T> PreBuiltCache<T> getCache(CachingStrategy cachingStrategy) {
        return switch (cachingStrategy) {
            case ONE -> new PreBuiltCacheStrategyOne<>();
            case LUCENE -> new PreBuiltCacheStrategyLucene<>();
            case INDEX -> new PreBuiltCacheStrategyElasticsearch<>();
        };
    }

    /**
     * This is a pretty simple cache, it only contains one version
     */
    private static class PreBuiltCacheStrategyOne<T> implements PreBuiltCache<T> {

        private T model = null;

        @Override
        public T get(IndexVersion version) {
            return model;
        }

        @Override
        public void put(IndexVersion version, T model) {
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

        private final Map<IndexVersion, T> mapModel = Maps.newMapWithExpectedSize(2);

        @Override
        public T get(IndexVersion version) {
            return mapModel.get(version);
        }

        @Override
        public void put(IndexVersion version, T model) {
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

        private final Map<org.apache.lucene.util.Version, T> mapModel = Maps.newMapWithExpectedSize(2);

        @Override
        public T get(IndexVersion version) {
            return mapModel.get(version.luceneVersion());
        }

        @Override
        public void put(IndexVersion version, T model) {
            mapModel.put(version.luceneVersion(), model);
        }

        @Override
        public Collection<T> values() {
            return mapModel.values();
        }
    }
}
