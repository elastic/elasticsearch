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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.UnmodifiableIterator;
import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.analysis.FieldNameAnalyzer;

import java.util.Set;

/**
 *
 */
public class DocumentFieldMappers implements Iterable<FieldMapper> {

    private final DocumentMapper docMapper;
    private final FieldMappersLookup fieldMappers;

    private volatile FieldNameAnalyzer indexAnalyzer;
    private volatile FieldNameAnalyzer searchAnalyzer;
    private volatile FieldNameAnalyzer searchQuoteAnalyzer;

    public DocumentFieldMappers(DocumentMapper docMapper) {
        this.docMapper = docMapper;
        this.fieldMappers = new FieldMappersLookup();
        this.indexAnalyzer = new FieldNameAnalyzer(ImmutableOpenMap.<String, Analyzer>of(), docMapper.indexAnalyzer());
        this.searchAnalyzer = new FieldNameAnalyzer(ImmutableOpenMap.<String, Analyzer>of(), docMapper.searchAnalyzer());
        this.searchQuoteAnalyzer = new FieldNameAnalyzer(ImmutableOpenMap.<String, Analyzer>of(), docMapper.searchQuotedAnalyzer());
    }

    public void addNewMappers(Iterable<FieldMapper> newMappers) {
        fieldMappers.addNewMappers(newMappers);

        final ImmutableOpenMap.Builder<String, Analyzer> indexAnalyzers = ImmutableOpenMap.builder(this.indexAnalyzer.analyzers());
        final ImmutableOpenMap.Builder<String, Analyzer> searchAnalyzers = ImmutableOpenMap.builder(this.searchAnalyzer.analyzers());
        final ImmutableOpenMap.Builder<String, Analyzer> searchQuoteAnalyzers = ImmutableOpenMap.builder(this.searchQuoteAnalyzer.analyzers());

        for (FieldMapper fieldMapper : newMappers) {
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

        this.indexAnalyzer = new FieldNameAnalyzer(indexAnalyzers.build(), docMapper.indexAnalyzer());
        this.searchAnalyzer = new FieldNameAnalyzer(searchAnalyzers.build(), docMapper.searchAnalyzer());
        this.searchQuoteAnalyzer = new FieldNameAnalyzer(searchQuoteAnalyzers.build(), docMapper.searchQuotedAnalyzer());
    }

    @Override
    public UnmodifiableIterator<FieldMapper> iterator() {
        return fieldMappers.iterator();
    }

    public ImmutableList<FieldMapper> mappers() {
        return this.fieldMappers.mappers();
    }

    public boolean hasMapper(FieldMapper fieldMapper) {
        return fieldMappers.mappers().contains(fieldMapper);
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

    public FieldMapper smartNameFieldMapper(String name) {
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
}
