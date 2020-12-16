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
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.Version;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.instanceOf;

public class MappingLookupTests extends ESTestCase {

    public void testOnlyRuntimeField() {
        MappingLookup mappingLookup = new MappingLookup("_doc", Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
            Collections.singletonList(new TestRuntimeField("test", "type")), 0, null, false);
        assertEquals(0, size(mappingLookup.fieldMappers()));
        assertEquals(0, mappingLookup.objectMappers().size());
        assertNull(mappingLookup.getMapper("test"));
        assertThat(mappingLookup.fieldTypes().get("test"), instanceOf(TestRuntimeField.class));
    }

    public void testRuntimeFieldLeafOverride() {
        MockFieldMapper fieldMapper = new MockFieldMapper("test");
        MappingLookup mappingLookup = new MappingLookup("_doc", Collections.singletonList(fieldMapper), Collections.emptyList(),
            Collections.emptyList(), Collections.singletonList(new TestRuntimeField("test", "type")), 0, null, false);
        assertThat(mappingLookup.getMapper("test"), instanceOf(MockFieldMapper.class));
        assertEquals(1, size(mappingLookup.fieldMappers()));
        assertEquals(0, mappingLookup.objectMappers().size());
        assertThat(mappingLookup.fieldTypes().get("test"), instanceOf(TestRuntimeField.class));
        assertEquals(1, size(mappingLookup.fieldTypes().filter(ft -> true)));
    }

    public void testSubfieldOverride() {
        MockFieldMapper fieldMapper = new MockFieldMapper("object.subfield");
        ObjectMapper objectMapper = new ObjectMapper("object", "object", new Explicit<>(true, true), ObjectMapper.Nested.NO,
            ObjectMapper.Dynamic.TRUE, Collections.singletonMap("object.subfield", fieldMapper), Version.CURRENT);
        MappingLookup mappingLookup = new MappingLookup(
            "_doc",
            Collections.singletonList(fieldMapper),
            Collections.singletonList(objectMapper),
            Collections.emptyList(),
            Collections.singletonList(new TestRuntimeField("object.subfield", "type")),
            0,
            null,
            false
        );
        assertThat(mappingLookup.getMapper("object.subfield"), instanceOf(MockFieldMapper.class));
        assertEquals(1, size(mappingLookup.fieldMappers()));
        assertEquals(1, mappingLookup.objectMappers().size());
        assertThat(mappingLookup.fieldTypes().get("object.subfield"), instanceOf(TestRuntimeField.class));
        assertEquals(1, size(mappingLookup.fieldTypes().filter(ft -> true)));
    }


    public void testAnalyzers() throws IOException {
        FakeFieldType fieldType1 = new FakeFieldType("field1");
        FieldMapper fieldMapper1 = new FakeFieldMapper(fieldType1, "index1");

        FakeFieldType fieldType2 = new FakeFieldType("field2");
        FieldMapper fieldMapper2 = new FakeFieldMapper(fieldType2, "index2");

        MappingLookup mappingLookup = new MappingLookup(
            "_doc",
            Arrays.asList(fieldMapper1, fieldMapper2),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            0,
            null,
            false
        );

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

    private static int size(Iterable<?> iterable) {
        int count = 0;
        for (Object obj : iterable) {
            count++;
        }
        return count;
    }

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
}
