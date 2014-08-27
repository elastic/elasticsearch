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
import com.google.common.collect.ForwardingList;
import com.google.common.collect.Maps;
import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.index.analysis.FieldNameAnalyzer;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public final class DocumentFieldMappers extends ForwardingList<FieldMapper<?>> {

    private final FieldMappersLookup fieldMappers;

    private final FieldNameAnalyzer indexAnalyzer;
    private final FieldNameAnalyzer searchAnalyzer;
    private final FieldNameAnalyzer searchQuoteAnalyzer;

    public DocumentFieldMappers(DocumentMapper docMapper) {
        this(new FieldMappersLookup(), new FieldNameAnalyzer(docMapper.indexAnalyzer()), new FieldNameAnalyzer(docMapper.searchAnalyzer()), new FieldNameAnalyzer(docMapper.searchQuotedAnalyzer()));
    }

    private DocumentFieldMappers(FieldMappersLookup fieldMappers, FieldNameAnalyzer indexAnalyzer, FieldNameAnalyzer searchAnalyzer, FieldNameAnalyzer searchQuoteAnalyzer) {
        this.fieldMappers = fieldMappers;
        this.indexAnalyzer = indexAnalyzer;
        this.searchAnalyzer = searchAnalyzer;
        this.searchQuoteAnalyzer = searchQuoteAnalyzer;
    }

    public DocumentFieldMappers copyAndAllAll(Collection<? extends FieldMapper<?>> newMappers) {
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

    public FieldMappers name(String name) {
        return fieldMappers.name(name);
    }

    public FieldMappers indexName(String indexName) {
        return fieldMappers.indexName(indexName);
    }

    public FieldMappers fullName(String fullName) {
        return fieldMappers.fullName(fullName);
    }

    public Set<String> simpleMatchToIndexNames(String pattern) {
        return fieldMappers.simpleMatchToIndexNames(pattern);
    }

    public Set<String> simpleMatchToFullName(String pattern) {
        return fieldMappers.simpleMatchToFullName(pattern);
    }

    /**
     * Tries to find first based on {@link #fullName(String)}, then by {@link #indexName(String)}, and last
     * by {@link #name(String)}.
     */
    public FieldMappers smartName(String name) {
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
        return indexAnalyzer.copyWithDefaultAnalyzer(defaultAnalyzer);
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

    @Override
    protected List<FieldMapper<?>> delegate() {
        return fieldMappers;
    }
}
