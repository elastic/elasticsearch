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

import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.Mapper.TypeParser.ParserContext;
import org.elasticsearch.search.NestedDocuments;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Collections.singleton;
import static java.util.Collections.unmodifiableSet;

/**
 * A {@link MapperService.Snapshot} with the "central" methods that are useful for testing.
 */
public class StubSnapshot implements MapperService.Snapshot {
    private final Function<String, MappedFieldType> lookup;
    private final Supplier<Set<String>> fields;

    public StubSnapshot(Function<String, MappedFieldType> lookup) {
        this.lookup = lookup;
        this.fields = () -> { throw new UnsupportedOperationException(); };
    }

    public StubSnapshot(Map<String, MappedFieldType> lookup) {
        this.lookup = lookup::get;
        this.fields = lookup::keySet;
    }

    @Override
    public MappedFieldType fieldType(String fullName) {
        return lookup.apply(fullName);
    }

    @Override
    public Set<String> simpleMatchToFullName(String pattern) {
        if (Regex.isSimpleMatchPattern(pattern) == false) {
            return singleton(pattern);
        }
        if (Regex.isMatchAllPattern(pattern)) {
            return unmodifiableSet(fields.get());
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasNested() {
        return false;
    }

    @Override
    public boolean hasMappings() {
        return true;
    }

    @Override
    public boolean sourceEnabled() {
        return true;
    }

    @Override
    public ObjectMapper getObjectMapper(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> sourcePath(String fullName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DocumentMapperForType documentMapperWithAutoCreate() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<MappedFieldType> getEagerGlobalOrdinalsFields() {
        throw new UnsupportedOperationException();
    }

    @Override
    public NamedAnalyzer indexAnalyzer(String field, Function<String, NamedAnalyzer> unindexedFieldAnalyzer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsBrokenAnalysis(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long version() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ParsedDocument parseDocument(SourceToParse source) throws MapperParsingException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ParserContext parserContext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isMetadataField(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexAnalyzers getIndexAnalyzers() {
        throw new UnsupportedOperationException();
    }

    @Override
    public NestedDocuments getNestedDocuments(Function<Query, BitSetProducer> filterProducer) {
        throw new UnsupportedOperationException();
    }
}
