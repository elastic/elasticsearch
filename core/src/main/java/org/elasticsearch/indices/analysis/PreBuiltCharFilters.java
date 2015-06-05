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
package org.elasticsearch.indices.analysis;

import org.apache.lucene.analysis.charfilter.HTMLStripCharFilter;
import org.elasticsearch.Version;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.indices.analysis.PreBuiltCacheFactory.CachingStrategy;

import java.io.Reader;
import java.util.Locale;

/**
 *
 */
public enum PreBuiltCharFilters {

    HTML_STRIP(CachingStrategy.ONE) {
        @Override
        public Reader create(Reader tokenStream, Version version) {
            return new HTMLStripCharFilter(tokenStream);
        }
    };

    abstract public Reader create(Reader tokenStream, Version version);

    protected final PreBuiltCacheFactory.PreBuiltCache<CharFilterFactory> cache;

    PreBuiltCharFilters(CachingStrategy cachingStrategy) {
        cache = PreBuiltCacheFactory.getCache(cachingStrategy);
    }

    public synchronized CharFilterFactory getCharFilterFactory(final Version version) {
        CharFilterFactory charFilterFactory = cache.get(version);
        if (charFilterFactory == null) {
            final String finalName = name();

            charFilterFactory = new CharFilterFactory() {
                @Override
                public String name() {
                    return finalName.toLowerCase(Locale.ROOT);
                }

                @Override
                public Reader create(Reader tokenStream) {
                    return valueOf(finalName).create(tokenStream, version);
                }
            };
            cache.put(version, charFilterFactory);
        }

        return charFilterFactory;
    }

    /**
     * Get a pre built CharFilter by its name or fallback to the default one
     * @param name CharFilter name
     * @param defaultCharFilter default CharFilter if name not found
     */
    public static PreBuiltCharFilters getOrDefault(String name, PreBuiltCharFilters defaultCharFilter) {
        try {
            return valueOf(name.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return defaultCharFilter;
        }
    }
}
