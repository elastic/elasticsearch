/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.TokenFilter;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.analysis.PreBuiltCacheFactory.CachingStrategy;

import java.io.Reader;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Provides pre-configured, shared {@link CharFilter}s.
 */
public class PreConfiguredCharFilter extends PreConfiguredAnalysisComponent<CharFilterFactory> {
    /**
     * Create a pre-configured char filter that may not vary at all.
     */
    public static PreConfiguredCharFilter singleton(String name, boolean useFilterForMultitermQueries, Function<Reader, Reader> create) {
        return new PreConfiguredCharFilter(
            name,
            CachingStrategy.ONE,
            useFilterForMultitermQueries,
            (reader, version) -> create.apply(reader)
        );
    }

    /**
     * Create a pre-configured char filter that may not vary at all, provide access to the index version
     */
    public static PreConfiguredCharFilter singletonWithVersion(
        String name,
        boolean useFilterForMultitermQueries,
        BiFunction<Reader, IndexVersion, Reader> create
    ) {
        return new PreConfiguredCharFilter(name, CachingStrategy.ONE, useFilterForMultitermQueries, create);
    }

    /**
     * Create a pre-configured token filter that may vary based on the Lucene version.
     */
    public static PreConfiguredCharFilter luceneVersion(
        String name,
        boolean useFilterForMultitermQueries,
        BiFunction<Reader, org.apache.lucene.util.Version, Reader> create
    ) {
        return new PreConfiguredCharFilter(
            name,
            CachingStrategy.LUCENE,
            useFilterForMultitermQueries,
            (reader, version) -> create.apply(reader, version.luceneVersion())
        );
    }

    /**
     * Create a pre-configured token filter that may vary based on the index version.
     */
    public static PreConfiguredCharFilter indexVersion(
        String name,
        boolean useFilterForMultitermQueries,
        BiFunction<Reader, IndexVersion, Reader> create
    ) {
        return new PreConfiguredCharFilter(name, CachingStrategy.INDEX, useFilterForMultitermQueries, create);
    }

    private final boolean useFilterForMultitermQueries;
    private final BiFunction<Reader, IndexVersion, Reader> create;

    protected PreConfiguredCharFilter(
        String name,
        CachingStrategy cache,
        boolean useFilterForMultitermQueries,
        BiFunction<Reader, IndexVersion, Reader> create
    ) {
        super(name, cache);
        this.useFilterForMultitermQueries = useFilterForMultitermQueries;
        this.create = create;
    }

    /**
     * Can this {@link TokenFilter} be used in multi-term queries?
     */
    public boolean shouldUseFilterForMultitermQueries() {
        return useFilterForMultitermQueries;
    }

    @Override
    protected CharFilterFactory create(IndexVersion version) {
        if (useFilterForMultitermQueries) {
            return new NormalizingCharFilterFactory() {
                @Override
                public String name() {
                    return getName();
                }

                @Override
                public Reader create(Reader reader) {
                    return create.apply(reader, version);
                }
            };
        }
        return new CharFilterFactory() {
            @Override
            public Reader create(Reader reader) {
                return create.apply(reader, version);
            }

            @Override
            public String name() {
                return getName();
            }
        };
    }

}
