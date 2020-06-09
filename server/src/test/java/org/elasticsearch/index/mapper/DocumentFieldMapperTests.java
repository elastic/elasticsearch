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
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.LuceneTestCase;
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

        FakeFieldType(String name) {
            super(name, true, true, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
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

        FakeFieldMapper(FakeFieldType fieldType) {
            super(fieldType.name(), new FieldType(), fieldType, MultiFields.empty(), CopyTo.empty());
        }

        @Override
        protected void parseCreateField(ParseContext context) throws IOException {
        }

        @Override
        protected Object parseSourceValue(Object value, String format) {
            throw new UnsupportedOperationException();
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
        FakeFieldType fieldType1 = new FakeFieldType("field1");
        fieldType1.setIndexAnalyzer(new NamedAnalyzer("foo", AnalyzerScope.INDEX, new FakeAnalyzer("index")));
        FieldMapper fieldMapper1 = new FakeFieldMapper(fieldType1);

        FakeFieldType fieldType2 = new FakeFieldType("field2");
        FieldMapper fieldMapper2 = new FakeFieldMapper(fieldType2);

        Analyzer defaultIndex = new FakeAnalyzer("default_index");

        DocumentFieldMappers documentFieldMappers = new DocumentFieldMappers(
            Arrays.asList(fieldMapper1, fieldMapper2),
            Collections.emptyList(),
            defaultIndex);

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
