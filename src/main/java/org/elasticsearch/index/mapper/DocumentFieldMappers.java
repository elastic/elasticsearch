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

package org.elasticsearch.index.mapper;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.analysis.FieldNameAnalyzer;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *
 */
public final class DocumentFieldMappers implements Iterable<FieldMapper<?>> {

    private final FieldMappersLookup fieldMappers;

    private final FieldNameAnalyzer indexAnalyzer;
    private final FieldNameAnalyzer searchAnalyzer;
    private final FieldNameAnalyzer searchQuoteAnalyzer;

    public DocumentFieldMappers(AnalysisService analysisService) {
        this(new FieldMappersLookup(), new FieldNameAnalyzer(analysisService.defaultIndexAnalyzer()),
                                       new FieldNameAnalyzer(analysisService.defaultSearchAnalyzer()),
                                       new FieldNameAnalyzer(analysisService.defaultSearchQuoteAnalyzer()));
    }

    private DocumentFieldMappers(FieldMappersLookup fieldMappers, FieldNameAnalyzer indexAnalyzer, FieldNameAnalyzer searchAnalyzer, FieldNameAnalyzer searchQuoteAnalyzer) {
        this.fieldMappers = fieldMappers;
        this.indexAnalyzer = indexAnalyzer;
        this.searchAnalyzer = searchAnalyzer;
        this.searchQuoteAnalyzer = searchQuoteAnalyzer;
    }

    public DocumentFieldMappers copyAndAllAll(Collection<FieldMapper<?>> newMappers) {
        FieldMappersLookup fieldMappers = this.fieldMappers.copyAndAddAll(newMappers);
        FieldNameAnalyzer indexAnalyzer = this.indexAnalyzer.copyAndAddAll(Collections2.transform(newMappers, new Function<FieldMapper<?>, Map.Entry<String, Analyzer>>() {
            @Override
            public Map.Entry<String, Analyzer> apply(FieldMapper<?> input) {
                return Maps.immutableEntry(input.names().indexName(), input.indexAnalyzer());
            }
        }));
        FieldNameAnalyzer searchAnalyzer = this.searchAnalyzer.copyAndAddAll(Collections2.transform(newMappers, new Function<FieldMapper<?>, Map.Entry<String, Analyzer>>() {
            @Override
            public Map.Entry<String, Analyzer> apply(FieldMapper<?> input) {
                return Maps.immutableEntry(input.names().indexName(), input.searchAnalyzer());
            }
        }));
        FieldNameAnalyzer searchQuoteAnalyzer = this.searchQuoteAnalyzer.copyAndAddAll(Collections2.transform(newMappers, new Function<FieldMapper<?>, Map.Entry<String, Analyzer>>() {
            @Override
            public Map.Entry<String, Analyzer> apply(FieldMapper<?> input) {
                return Maps.immutableEntry(input.names().indexName(), input.searchQuoteAnalyzer());
            }
        }));
        return new DocumentFieldMappers(fieldMappers, indexAnalyzer, searchAnalyzer, searchQuoteAnalyzer);
    }

    /**
     * Looks up a field by its index name.
     *
     * Overriding index name for a field is no longer possibly, and only supported for backcompat.
     * This function first attempts to lookup the field by full name, and only when that fails,
     * does a full scan of all field mappers, collecting those with this index name.
     *
     * This will be removed in 3.0, once backcompat for overriding index name is removed.
     * @deprecated Use {@link #getMapper(String)}
     */
    @Deprecated
    public FieldMappers indexName(String indexName) {
        return fieldMappers.indexName(indexName);
    }

    /** Returns the mapper for the given field */
    public FieldMapper getMapper(String field) {
        return fieldMappers.get(field);
    }

    List<String> simpleMatchToIndexNames(String pattern) {
        return fieldMappers.simpleMatchToIndexNames(pattern);
    }

    public List<String> simpleMatchToFullName(String pattern) {
        return fieldMappers.simpleMatchToFullName(pattern);
    }

    /**
     * Tries to find first based on fullName, then by indexName.
     */
    FieldMappers smartName(String name) {
        return fieldMappers.smartName(name);
    }

    public FieldMapper<?> smartNameFieldMapper(String name) {
        return fieldMappers.smartNameFieldMapper(name);
    }

    /**
     * A smart analyzer used for indexing that takes into account specific analyzers configured
     * per {@link FieldMapper}.
     */
    public Analyzer indexAnalyzer() {
        return this.indexAnalyzer;
    }

    /**
     * A smart analyzer used for indexing that takes into account specific analyzers configured
     * per {@link FieldMapper} with a custom default analyzer for no explicit field analyzer.
     */
    public Analyzer indexAnalyzer(Analyzer defaultAnalyzer) {
        return new FieldNameAnalyzer(indexAnalyzer.analyzers(), defaultAnalyzer);
    }

    /**
     * A smart analyzer used for searching that takes into account specific analyzers configured
     * per {@link FieldMapper}.
     */
    public Analyzer searchAnalyzer() {
        return this.searchAnalyzer;
    }

    public Analyzer searchQuoteAnalyzer() {
        return this.searchQuoteAnalyzer;
    }

    public Iterator<FieldMapper<?>> iterator() {
        return fieldMappers.iterator();
    }
}
