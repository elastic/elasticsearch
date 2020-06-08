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
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class DocumentFieldMapperTests extends LuceneTestCase {

    private static class FakeAnalyzer extends Analyzer {

        private final String output;

        FakeAnalyzer(String output) {
            this.output = output;
        }

        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer tokenizer = new Tokenizer() {
                boolean incremented = false;
                CharTermAttribute term = addAttribute(CharTermAttribute.class);

                @Override
                public boolean incrementToken() throws IOException {
                    if (incremented) {
                        return false;
                    }
                    term.setLength(0).append(output);
                    incremented = true;
                    return true;
                }
            };
            return new TokenStreamComponents(tokenizer);
        }

    }

    static class FakeFieldType extends TermBasedFieldType {

        FakeFieldType() {
            super();
        }

        FakeFieldType(FakeFieldType other) {
            super(other);
        }

        @Override
        public MappedFieldType clone() {
            return new FakeFieldType(this);
        }

        @Override
        public String typeName() {
            return "fake";
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            if (hasDocValues()) {
                return new DocValuesFieldExistsQuery(name());
            } else {
                return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
            }
        }

    }

    static class FakeFieldMapper extends FieldMapper {

        private static final Settings SETTINGS = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();

        FakeFieldMapper(String simpleName, MappedFieldType fieldType) {
            super(simpleName, fieldType.clone(), fieldType.clone(), SETTINGS, MultiFields.empty(), CopyTo.empty());
        }

        @Override
        protected void parseCreateField(ParseContext context) throws IOException {
        }

        @Override
        protected void mergeOptions(FieldMapper other, List<String> conflicts) {

        }

        @Override
        protected String contentType() {
            return null;
        }

    }

    public void testAnalyzers() throws IOException {
        FakeFieldType fieldType1 = new FakeFieldType();
        fieldType1.setName("field1");
        fieldType1.setIndexAnalyzer(new NamedAnalyzer("foo", AnalyzerScope.INDEX, new FakeAnalyzer("index")));
        fieldType1.setSearchAnalyzer(new NamedAnalyzer("bar", AnalyzerScope.INDEX, new FakeAnalyzer("search")));
        fieldType1.setSearchQuoteAnalyzer(new NamedAnalyzer("baz", AnalyzerScope.INDEX, new FakeAnalyzer("search_quote")));
        FieldMapper fieldMapper1 = new FakeFieldMapper("field1", fieldType1);

        FakeFieldType fieldType2 = new FakeFieldType();
        fieldType2.setName("field2");
        FieldMapper fieldMapper2 = new FakeFieldMapper("field2", fieldType2);

        Analyzer defaultIndex = new FakeAnalyzer("default_index");
        Analyzer defaultSearch = new FakeAnalyzer("default_search");
        Analyzer defaultSearchQuote = new FakeAnalyzer("default_search_quote");

        DocumentFieldMappers documentFieldMappers = new DocumentFieldMappers(
            Arrays.asList(fieldMapper1, fieldMapper2),
            Collections.emptyList(),
            defaultIndex,
            defaultSearch,
            defaultSearchQuote);

        assertAnalyzes(documentFieldMappers.indexAnalyzer(), "field1", "index");

        assertAnalyzes(documentFieldMappers.indexAnalyzer(), "field2", "default_index");
    }

    private void assertAnalyzes(Analyzer analyzer, String field, String output) throws IOException {
        try (TokenStream tok = analyzer.tokenStream(field, new StringReader(""))) {
            CharTermAttribute term = tok.addAttribute(CharTermAttribute.class);
            assertTrue(tok.incrementToken());
            assertEquals(output, term.toString());
        }
    }
}
