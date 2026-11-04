/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.analysis.PreBuiltAnalyzers;
import org.elasticsearch.indices.analysis.PreBuiltCacheFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class PreBuiltAnalyzerProviderFactory extends PreConfiguredAnalysisComponent<AnalyzerProvider<?>> implements Closeable {

    private final Function<IndexVersion, Analyzer> create;
    private final boolean versionInvariant;
    private final PreBuiltAnalyzerProvider current;

    /**
     * This constructor only exists to expose analyzers defined in {@link PreBuiltAnalyzers} as {@link PreBuiltAnalyzerProviderFactory}.
     */
    PreBuiltAnalyzerProviderFactory(String name, PreBuiltAnalyzers preBuiltAnalyzer) {
        super(name, new PreBuiltAnalyzersDelegateCache(name, preBuiltAnalyzer));
        this.create = preBuiltAnalyzer::getAnalyzer;
        this.versionInvariant = true;
        Analyzer analyzer = preBuiltAnalyzer.getAnalyzer(IndexVersion.current());
        current = new PreBuiltAnalyzerProvider(name, AnalyzerScope.INDICES, analyzer);
    }

    /**
     * For analyzers whose behavior does not depend on the index version; instances are shared across indices by name.
     */
    public PreBuiltAnalyzerProviderFactory(String name, PreBuiltCacheFactory.CachingStrategy cache, Supplier<Analyzer> create) {
        this(name, PreBuiltCacheFactory.getCache(cache), version -> create.get(), true);
    }

    private PreBuiltAnalyzerProviderFactory(
        String name,
        PreBuiltCacheFactory.PreBuiltCache<AnalyzerProvider<?>> cache,
        Function<IndexVersion, Analyzer> create,
        boolean versionInvariant
    ) {
        super(name, cache);
        this.create = create;
        this.versionInvariant = versionInvariant;
        this.current = newProvider(name, create.apply(IndexVersion.current()), versionInvariant);
        if (versionInvariant == false) {
            cache.put(IndexVersion.current(), current);
        }
    }

    /**
     * For analyzers whose behavior is grouped into caller-defined versions. The version key determines both cache identity and the
     * value passed to {@code create}, so behaviorally equivalent index versions share one provider.
     */
    public static <V> PreBuiltAnalyzerProviderFactory versioned(
        String name,
        Function<IndexVersion, V> version,
        Function<V, Analyzer> create
    ) {
        return new PreBuiltAnalyzerProviderFactory(
            name,
            PreBuiltCacheFactory.getCache(version),
            indexVersion -> create.apply(version.apply(indexVersion)),
            false
        );
    }

    @Override
    public AnalyzerProvider<?> get(IndexSettings indexSettings, Environment environment, String name, Settings settings)
        throws IOException {
        IndexVersion versionCreated = indexSettings.getIndexVersionCreated();
        if (IndexVersion.current().equals(versionCreated) == false) {
            return super.get(indexSettings, environment, name, settings);
        } else {
            return current;
        }
    }

    @Override
    protected AnalyzerProvider<?> create(IndexVersion version) {
        assert IndexVersion.current().equals(version) == false;
        return newProvider(name, create.apply(version), versionInvariant);
    }

    private static PreBuiltAnalyzerProvider newProvider(String name, Analyzer analyzer, boolean versionInvariant) {
        return versionInvariant
            ? new PreBuiltAnalyzerProvider(name, AnalyzerScope.INDICES, analyzer)
            : new CacheKeyedPreBuiltAnalyzerProvider(name, analyzer);
    }

    @Override
    public void close() throws IOException {
        List<Closeable> closeables = cache.values().stream().<Closeable>map(AnalyzerProvider::get).toList();
        if (versionInvariant == false) {
            IOUtils.close(closeables);
        } else {
            IOUtils.close(current.get(), () -> IOUtils.close(closeables));
        }
    }

    /**
     * The versioned cache canonicalizes each behavior key to one provider, so provider identity is also the analyzer sharing key.
     * Keeping this behavior on a distinct provider type matches {@link AnalysisRegistry.FactoryKey}, which isolates sharing keys by
     * their provider class.
     */
    private static final class CacheKeyedPreBuiltAnalyzerProvider extends PreBuiltAnalyzerProvider {

        private CacheKeyedPreBuiltAnalyzerProvider(String name, Analyzer analyzer) {
            super(name, AnalyzerScope.INDICES, analyzer);
        }

        @Override
        public Object sharingKey() {
            return this;
        }
    }

    /**
     *  A special cache that closes the gap between PreBuiltAnalyzers and PreBuiltAnalyzerProviderFactory.
     *
     *  This can be removed when all analyzers have been moved away from PreBuiltAnalyzers to
     *  PreBuiltAnalyzerProviderFactory either in server or analysis-common.
     */
    static class PreBuiltAnalyzersDelegateCache implements PreBuiltCacheFactory.PreBuiltCache<AnalyzerProvider<?>> {

        private final String name;
        private final PreBuiltAnalyzers preBuiltAnalyzer;

        private PreBuiltAnalyzersDelegateCache(String name, PreBuiltAnalyzers preBuiltAnalyzer) {
            this.name = name;
            this.preBuiltAnalyzer = preBuiltAnalyzer;
        }

        @Override
        public AnalyzerProvider<?> get(IndexVersion version) {
            return new PreBuiltAnalyzerProvider(name, AnalyzerScope.INDICES, preBuiltAnalyzer.getAnalyzer(version));
        }

        @Override
        public void put(IndexVersion version, AnalyzerProvider<?> analyzerProvider) {
            // No need to put, because we delegate in get() directly to PreBuiltAnalyzers which already caches.
        }

        @Override
        public Collection<AnalyzerProvider<?>> values() {
            return preBuiltAnalyzer.getCache()
                .values()
                .stream()
                // Wrap the analyzer instance in a PreBuiltAnalyzerProvider, this is what PreBuiltAnalyzerProviderFactory#close expects
                // (other caches are not directly caching analyzers, but analyzer provider instead)
                .<AnalyzerProvider<?>>map(analyzer -> new PreBuiltAnalyzerProvider(name, AnalyzerScope.INDICES, analyzer))
                .toList();
        }

    }
}
