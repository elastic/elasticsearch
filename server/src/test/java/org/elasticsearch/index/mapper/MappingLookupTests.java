/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.instanceOf;

public class MappingLookupTests extends ESTestCase {

    private static MappingLookup createMappingLookup(List<FieldMapper> fieldMappers,
                                                     List<ObjectMapper> objectMappers,
                                                     List<RuntimeField> runtimeFields) {
        RootObjectMapper.Builder builder = new RootObjectMapper.Builder("_doc");
        Map<String, RuntimeField> runtimeFieldTypes = runtimeFields.stream().collect(Collectors.toMap(RuntimeField::name, r -> r));
        builder.setRuntime(runtimeFieldTypes);
        Mapping mapping = new Mapping(
            builder.build(new ContentPath()),
            new MetadataFieldMapper[0],
            Collections.emptyMap(),
            randomBoolean()
        );
        return MappingLookup.fromMappers(mapping, fieldMappers, objectMappers, emptyList());
    }

    public void testOnlyRuntimeField() {
        MappingLookup mappingLookup = createMappingLookup(emptyList(), emptyList(),
            Collections.singletonList(new TestRuntimeField("test", "type")));
        assertEquals(0, size(mappingLookup.fieldMappers()));
        assertEquals(0, mappingLookup.objectMappers().size());
        assertNull(mappingLookup.getMapper("test"));
        assertThat(mappingLookup.fieldTypesLookup().get("test"), instanceOf(TestRuntimeField.TestRuntimeFieldType.class));
    }

    public void testRuntimeFieldLeafOverride() {
        MockFieldMapper fieldMapper = new MockFieldMapper("test");
        MappingLookup mappingLookup = createMappingLookup(Collections.singletonList(fieldMapper), emptyList(),
            Collections.singletonList(new TestRuntimeField("test", "type")));
        assertThat(mappingLookup.getMapper("test"), instanceOf(MockFieldMapper.class));
        assertEquals(1, size(mappingLookup.fieldMappers()));
        assertEquals(0, mappingLookup.objectMappers().size());
        assertThat(mappingLookup.fieldTypesLookup().get("test"), instanceOf(TestRuntimeField.TestRuntimeFieldType.class));
    }

    public void testSubfieldOverride() {
        MockFieldMapper fieldMapper = new MockFieldMapper("object.subfield");
        ObjectMapper objectMapper = new ObjectMapper("object", "object", new Explicit<>(true, true),
            ObjectMapper.Dynamic.TRUE, Collections.singletonMap("object.subfield", fieldMapper));
        MappingLookup mappingLookup = createMappingLookup(
            Collections.singletonList(fieldMapper),
            Collections.singletonList(objectMapper),
            Collections.singletonList(new TestRuntimeField("object.subfield", "type"))
        );
        assertThat(mappingLookup.getMapper("object.subfield"), instanceOf(MockFieldMapper.class));
        assertEquals(1, size(mappingLookup.fieldMappers()));
        assertEquals(1, mappingLookup.objectMappers().size());
        assertThat(mappingLookup.fieldTypesLookup().get("object.subfield"), instanceOf(TestRuntimeField.TestRuntimeFieldType.class));
    }

    public void testAnalyzers() throws IOException {
        FakeFieldType fieldType1 = new FakeFieldType("field1");
        FieldMapper fieldMapper1 = new FakeFieldMapper(fieldType1, "index1");

        FakeFieldType fieldType2 = new FakeFieldType("field2");
        FieldMapper fieldMapper2 = new FakeFieldMapper(fieldType2, "index2");

        MappingLookup mappingLookup = createMappingLookup(
            Arrays.asList(fieldMapper1, fieldMapper2),
            emptyList(),
            emptyList()
        );

        assertAnalyzes(mappingLookup.indexAnalyzer("field1", f -> null), "field1", "index1");
        assertAnalyzes(mappingLookup.indexAnalyzer("field2", f -> null), "field2", "index2");
        expectThrows(IllegalArgumentException.class,
            () -> mappingLookup.indexAnalyzer("field3", f -> {
                throw new IllegalArgumentException();
            }).tokenStream("field3", "blah"));
    }

    public void testEmptyMappingLookup() {
        MappingLookup mappingLookup = MappingLookup.EMPTY;
        assertEquals("{\"_doc\":{}}", Strings.toString(mappingLookup.getMapping()));
        assertFalse(mappingLookup.hasMappings());
        assertNull(mappingLookup.getMapping().getMeta());
        assertEquals(0, mappingLookup.getMapping().getMetadataMappersMap().size());
        assertFalse(mappingLookup.fieldMappers().iterator().hasNext());
        assertEquals(0, mappingLookup.getMatchingFieldNames("*").size());
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
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
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
        protected void parseCreateField(DocumentParserContext context) {
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
