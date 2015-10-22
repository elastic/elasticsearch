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

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.common.collect.CopyOnWriteHashMap;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.analysis.FieldNameAnalyzer;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public final class DocumentFieldMappers implements Iterable<FieldMapper> {

    /** Full field name to mapper */
    private final CopyOnWriteHashMap<String, FieldMapper> fieldMappers;

    private final FieldNameAnalyzer indexAnalyzer;
    private final FieldNameAnalyzer searchAnalyzer;
    private final FieldNameAnalyzer searchQuoteAnalyzer;

    public DocumentFieldMappers(AnalysisService analysisService) {
        this(new CopyOnWriteHashMap<String, FieldMapper>(),
             new FieldNameAnalyzer(analysisService.defaultIndexAnalyzer()),
             new FieldNameAnalyzer(analysisService.defaultSearchAnalyzer()),
             new FieldNameAnalyzer(analysisService.defaultSearchQuoteAnalyzer()));
    }

    private DocumentFieldMappers(CopyOnWriteHashMap<String, FieldMapper> fieldMappers, FieldNameAnalyzer indexAnalyzer, FieldNameAnalyzer searchAnalyzer, FieldNameAnalyzer searchQuoteAnalyzer) {
        this.fieldMappers = fieldMappers;
        this.indexAnalyzer = indexAnalyzer;
        this.searchAnalyzer = searchAnalyzer;
        this.searchQuoteAnalyzer = searchQuoteAnalyzer;
    }

    public DocumentFieldMappers copyAndAllAll(Collection<FieldMapper> newMappers) {
        CopyOnWriteHashMap<String, FieldMapper> map = this.fieldMappers;
        for (FieldMapper fieldMapper : newMappers) {
            map = map.copyAndPut(fieldMapper.fieldType().names().fullName(), fieldMapper);
        }
        FieldNameAnalyzer indexAnalyzer = this.indexAnalyzer.copyAndAddAll(newMappers.stream().map((input) ->
                new AbstractMap.SimpleImmutableEntry<>(input.fieldType().names().indexName(), (Analyzer)input.fieldType().indexAnalyzer())
        ));
        FieldNameAnalyzer searchAnalyzer = this.searchAnalyzer.copyAndAddAll(newMappers.stream().map((input) ->
                new AbstractMap.SimpleImmutableEntry<>(input.fieldType().names().indexName(), (Analyzer)input.fieldType().searchAnalyzer())
        ));
        FieldNameAnalyzer searchQuoteAnalyzer = this.searchQuoteAnalyzer.copyAndAddAll(newMappers.stream().map((input) ->
                new AbstractMap.SimpleImmutableEntry<>(input.fieldType().names().indexName(), (Analyzer) input.fieldType().searchQuoteAnalyzer())
        ));
        return new DocumentFieldMappers(map,indexAnalyzer,searchAnalyzer,searchQuoteAnalyzer);
    }

/** Returns the mapper for the given field */
    public FieldMapper getMapper(String field) {
        return fieldMappers.get(field);
    }

    public Collection<String> simpleMatchToFullName(String pattern) {
        Set<String> fields = new HashSet<>();
        for (FieldMapper fieldMapper : this) {
            if (Regex.simpleMatch(pattern, fieldMapper.fieldType().names().fullName())) {
                fields.add(fieldMapper.fieldType().names().fullName());
            } else if (Regex.simpleMatch(pattern, fieldMapper.fieldType().names().indexName())) {
                fields.add(fieldMapper.fieldType().names().fullName());
            }
        }
        return fields;
    }

    public FieldMapper smartNameFieldMapper(String name) {
        FieldMapper fieldMapper = getMapper(name);
        if (fieldMapper != null) {
            return fieldMapper;
        }
        for (FieldMapper otherFieldMapper : this) {
            if (otherFieldMapper.fieldType().names().indexName().equals(name)) {
                return otherFieldMapper;
            }
        }
        return null;
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

    public Iterator<FieldMapper> iterator() {
        return fieldMappers.values().iterator();
    }
}
