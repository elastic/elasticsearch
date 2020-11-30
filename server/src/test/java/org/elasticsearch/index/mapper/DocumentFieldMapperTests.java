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
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collections;

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
                final CharTermAttribute term = addAttribute(CharTermAttribute.class);

                @Override
                public boolean incrementToken() {
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

        private FakeFieldType(String name) {
            super(name, true, false, true, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, String format) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String typeName() {
            return "fake";
        }
    }

    static class FakeFieldMapper extends FieldMapper {

        final String indexedValue;

        FakeFieldMapper(FakeFieldType fieldType, String indexedValue) {
            super(fieldType.name(), fieldType,
                new NamedAnalyzer("fake", AnalyzerScope.INDEX, new FakeAnalyzer(indexedValue)),
                MultiFields.empty(), CopyTo.empty());
            this.indexedValue = indexedValue;
        }

        @Override
        protected void parseCreateField(ParseContext context) {
        }

        @Override
        protected String contentType() {
            return null;
        }

        @Override
        public Builder getMergeBuilder() {
            return null;
        }
    }

    public void testAnalyzers() throws IOException {
        FakeFieldType fieldType1 = new FakeFieldType("field1");
        FieldMapper fieldMapper1 = new FakeFieldMapper(fieldType1, "index1");

        FakeFieldType fieldType2 = new FakeFieldType("field2");
        FieldMapper fieldMapper2 = new FakeFieldMapper(fieldType2, "index2");

        MappingLookup mappingLookup = new MappingLookup(
            Arrays.asList(fieldMapper1, fieldMapper2),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            0);

        assertAnalyzes(mappingLookup.indexAnalyzer("field1", f -> null), "field1", "index1");
        assertAnalyzes(mappingLookup.indexAnalyzer("field2", f -> null), "field2", "index2");
        expectThrows(IllegalArgumentException.class,
            () -> mappingLookup.indexAnalyzer("field3", f -> {
                throw new IllegalArgumentException();
            }).tokenStream("field3", "blah"));
    }

    private void assertAnalyzes(Analyzer analyzer, String field, String output) throws IOException {
        try (TokenStream tok = analyzer.tokenStream(field, new StringReader(""))) {
            CharTermAttribute term = tok.addAttribute(CharTermAttribute.class);
            assertTrue(tok.incrementToken());
            assertEquals(output, term.toString());
        }
    }
}
