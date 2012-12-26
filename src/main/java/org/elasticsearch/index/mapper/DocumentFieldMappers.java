/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.*;
import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.analysis.FieldNameAnalyzer;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;

/**
 *
 */
public class DocumentFieldMappers implements Iterable<FieldMapper> {

    private final ImmutableList<FieldMapper> fieldMappers;
    private final Map<String, FieldMappers> fullNameFieldMappers;
    private final Map<String, FieldMappers> nameFieldMappers;
    private final Map<String, FieldMappers> indexNameFieldMappers;

    private final FieldNameAnalyzer indexAnalyzer;
    private final FieldNameAnalyzer searchAnalyzer;
    private final FieldNameAnalyzer searchQuoteAnalyzer;

    public DocumentFieldMappers(DocumentMapper docMapper, Iterable<FieldMapper> fieldMappers) {
        final Map<String, FieldMappers> tempNameFieldMappers = newHashMap();
        final Map<String, FieldMappers> tempIndexNameFieldMappers = newHashMap();
        final Map<String, FieldMappers> tempFullNameFieldMappers = newHashMap();

        final Map<String, Analyzer> indexAnalyzers = newHashMap();
        final Map<String, Analyzer> searchAnalyzers = newHashMap();
        final Map<String, Analyzer> searchQuoteAnalyzers = newHashMap();

        for (FieldMapper fieldMapper : fieldMappers) {
            FieldMappers mappers = tempNameFieldMappers.get(fieldMapper.names().name());
            if (mappers == null) {
                mappers = new FieldMappers(fieldMapper);
            } else {
                mappers = mappers.concat(fieldMapper);
            }
            tempNameFieldMappers.put(fieldMapper.names().name(), mappers);

            mappers = tempIndexNameFieldMappers.get(fieldMapper.names().indexName());
            if (mappers == null) {
                mappers = new FieldMappers(fieldMapper);
            } else {
                mappers = mappers.concat(fieldMapper);
            }
            tempIndexNameFieldMappers.put(fieldMapper.names().indexName(), mappers);

            mappers = tempFullNameFieldMappers.get(fieldMapper.names().fullName());
            if (mappers == null) {
                mappers = new FieldMappers(fieldMapper);
            } else {
                mappers = mappers.concat(fieldMapper);
            }
            tempFullNameFieldMappers.put(fieldMapper.names().fullName(), mappers);

            if (fieldMapper.indexAnalyzer() != null) {
                indexAnalyzers.put(fieldMapper.names().indexName(), fieldMapper.indexAnalyzer());
            }
            if (fieldMapper.searchAnalyzer() != null) {
                searchAnalyzers.put(fieldMapper.names().indexName(), fieldMapper.searchAnalyzer());
            }
            if (fieldMapper.searchQuoteAnalyzer() != null) {
                searchQuoteAnalyzers.put(fieldMapper.names().indexName(), fieldMapper.searchQuoteAnalyzer());
            }
        }
        this.fieldMappers = ImmutableList.copyOf(fieldMappers);
        this.nameFieldMappers = ImmutableMap.copyOf(tempNameFieldMappers);
        this.indexNameFieldMappers = ImmutableMap.copyOf(tempIndexNameFieldMappers);
        this.fullNameFieldMappers = ImmutableMap.copyOf(tempFullNameFieldMappers);

        this.indexAnalyzer = new FieldNameAnalyzer(indexAnalyzers, docMapper.indexAnalyzer());
        this.searchAnalyzer = new FieldNameAnalyzer(searchAnalyzers, docMapper.searchAnalyzer());
        this.searchQuoteAnalyzer = new FieldNameAnalyzer(searchQuoteAnalyzers, docMapper.searchQuotedAnalyzer());
    }

    @Override
    public UnmodifiableIterator<FieldMapper> iterator() {
        return fieldMappers.iterator();
    }

    public ImmutableList<FieldMapper> mappers() {
        return this.fieldMappers;
    }

    public boolean hasMapper(FieldMapper fieldMapper) {
        return fieldMappers.contains(fieldMapper);
    }

    public FieldMappers name(String name) {
        return nameFieldMappers.get(name);
    }

    public FieldMappers indexName(String indexName) {
        return indexNameFieldMappers.get(indexName);
    }

    public FieldMappers fullName(String fullName) {
        return fullNameFieldMappers.get(fullName);
    }

    public Set<String> simpleMatchToIndexNames(String pattern) {
        Set<String> fields = Sets.newHashSet();
        for (FieldMapper fieldMapper : fieldMappers) {
            if (Regex.simpleMatch(pattern, fieldMapper.names().fullName())) {
                fields.add(fieldMapper.names().indexName());
            } else if (Regex.simpleMatch(pattern, fieldMapper.names().indexName())) {
                fields.add(fieldMapper.names().indexName());
            } else if (Regex.simpleMatch(pattern, fieldMapper.names().name())) {
                fields.add(fieldMapper.names().indexName());
            }
        }
        return fields;
    }

    public Set<String> simpleMatchToFullName(String pattern) {
        Set<String> fields = Sets.newHashSet();
        for (FieldMapper fieldMapper : fieldMappers) {
            if (Regex.simpleMatch(pattern, fieldMapper.names().fullName())) {
                fields.add(fieldMapper.names().fullName());
            } else if (Regex.simpleMatch(pattern, fieldMapper.names().indexName())) {
                fields.add(fieldMapper.names().fullName());
            } else if (Regex.simpleMatch(pattern, fieldMapper.names().name())) {
                fields.add(fieldMapper.names().fullName());
            }
        }
        return fields;
    }

    /**
     * Tries to find first based on {@link #fullName(String)}, then by {@link #indexName(String)}, and last
     * by {@link #name(String)}.
     */
    public FieldMappers smartName(String name) {
        FieldMappers fieldMappers = fullName(name);
        if (fieldMappers != null) {
            return fieldMappers;
        }
        fieldMappers = indexName(name);
        if (fieldMappers != null) {
            return fieldMappers;
        }
        return name(name);
    }

    public FieldMapper smartNameFieldMapper(String name) {
        FieldMappers fieldMappers = smartName(name);
        if (fieldMappers == null) {
            return null;
        }
        return fieldMappers.mapper();
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

    public DocumentFieldMappers concat(DocumentMapper docMapper, FieldMapper... fieldMappers) {
        return concat(docMapper, newArrayList(fieldMappers));
    }

    public DocumentFieldMappers concat(DocumentMapper docMapper, Iterable<FieldMapper> fieldMappers) {
        return new DocumentFieldMappers(docMapper, Iterables.concat(this.fieldMappers, fieldMappers));
    }
}
