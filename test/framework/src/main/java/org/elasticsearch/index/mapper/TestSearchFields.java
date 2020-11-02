/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.elasticsearch.index.analysis.FieldNameAnalyzer;
import org.elasticsearch.index.analysis.IndexAnalyzers;

import java.util.Set;

/**
 * Test implementation of {@link SearchFields} that does not depend on {@link MapperService}.
 * Each test may override the methods that it needs.
 */
public class TestSearchFields extends SearchFields {

    public TestSearchFields() {
        super(null, true);
    }

    @Override
    public void setAllowUnmappedFields(boolean allowUnmappedFields) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setMapUnmappedFieldAsText(boolean mapUnmappedFieldAsText) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean mapUnmappedFieldAsText() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean allowUnmappedFields() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ParsedDocument parseDocument(SourceToParse source) throws MapperParsingException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FieldNameAnalyzer getFieldNameIndexAnalyzer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasNested() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasMappings() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> simpleMatchToIndexNames(String pattern) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MappedFieldType fieldType(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isFieldMapped(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ObjectMapper getObjectMapper(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MappedFieldType buildAnonymousFieldType(String type) {
        throw new UnsupportedOperationException();
    }

    @Override
    MappedFieldType failIfFieldMappingNotFound(String name, MappedFieldType fieldMapping) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexAnalyzers getIndexAnalyzers() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Analyzer getIndexAnalyzer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> sourcePath(String fieldName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean sourceEnabled() {
        return true;
    }
}
