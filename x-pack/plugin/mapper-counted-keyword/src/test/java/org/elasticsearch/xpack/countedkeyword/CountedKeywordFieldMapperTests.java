/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.countedkeyword;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class CountedKeywordFieldMapperTests extends MapperTestCase {
    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return Collections.singletonList(new CountedKeywordMapperPlugin());
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", CountedKeywordFieldMapper.CONTENT_TYPE);
    }

    @Override
    protected Object getSampleValueForDocument() {
        return new String[] { "a", "a", "b", "c" };
    }

    @Override
    protected Object getSampleValueForQuery() {
        return "b";
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return false;
    }

    @Override
    protected boolean supportsStoredFields() {
        return false;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) {
        // Nothing to do
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        return randomBoolean() ? null : randomAlphaOfLengthBetween(1, 10);
    }

    public void testSyntheticSourceSingleNullValue() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("field");
            minimalMapping(b);
            b.endObject();
        })).documentMapper();

        String expected = "{}";
        CheckedConsumer<XContentBuilder, IOException> buildInput = b -> {
            b.field("field");
            b.nullValue();
        };

        assertThat(syntheticSource(mapper, buildInput), equalTo(expected));
        assertThat(syntheticSource(mapper, new SourceFilter(new String[] { "field" }, null), buildInput), equalTo(expected));
        assertThat(syntheticSource(mapper, new SourceFilter(null, new String[] { "field" }), buildInput), equalTo("{}"));
    }

    public void testSyntheticSourceManyNullValue() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("field");
            minimalMapping(b);
            b.endObject();
        })).documentMapper();

        int nullCount = randomIntBetween(1, 5);

        String expected = "{}";
        CheckedConsumer<XContentBuilder, IOException> buildInput = b -> {
            b.startArray("field");
            for (int i = 0; i < nullCount; i++) {
                b.nullValue();
            }
            b.endArray();
        };

        assertThat(syntheticSource(mapper, buildInput), equalTo(expected));
        assertThat(syntheticSource(mapper, new SourceFilter(new String[] { "field" }, null), buildInput), equalTo(expected));
        assertThat(syntheticSource(mapper, new SourceFilter(null, new String[] { "field" }), buildInput), equalTo("{}"));
    }

    public void testSyntheticSourceIndexLevelKeepArrays() throws IOException {
        SyntheticSourceExample example = syntheticSourceSupportForKeepTests(shouldUseIgnoreMalformed(), Mapper.SourceKeepMode.ARRAYS)
            .example(1);
        XContentBuilder mappings = mapping(b -> {
            b.startObject("field");
            example.mapping().accept(b);
            b.endObject();
        });

        var settings = Settings.builder()
            .put("index.mapping.source.mode", "synthetic")
            .put("index.mapping.synthetic_source_keep", "arrays")
            .build();
        DocumentMapper mapperAll = createMapperService(getVersion(), settings, () -> true, mappings).documentMapper();

        int elementCount = randomIntBetween(2, 5);
        CheckedConsumer<XContentBuilder, IOException> buildInput = (XContentBuilder builder) -> {
            example.buildInputArray(builder, elementCount);
        };

        var builder = XContentFactory.jsonBuilder();
        builder.startObject();
        buildInput.accept(builder);
        builder.endObject();
        String expected = Strings.toString(builder);
        String actual = syntheticSource(mapperAll, buildInput);
        assertThat(actual, equalTo(expected));
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        return new SyntheticSourceSupport() {
            @Override
            public SyntheticSourceExample example(int maxValues) throws IOException {
                if (randomBoolean()) {
                    Tuple<String, String> v = generateValue();
                    return new SyntheticSourceExample(v.v1(), v.v2(), this::mapping);
                }
                int maxNullValues = 5;
                List<Tuple<String, String>> values = randomList(1, maxValues, this::generateValue);
                List<String> in = Stream.concat(values.stream().map(Tuple::v1), randomList(0, maxNullValues, () -> (String) null).stream())
                    .toList();

                in = shuffledList(in);

                List<String> outList = values.stream().map(Tuple::v2).sorted().toList();

                Object out = outList.size() == 1 ? outList.get(0) : outList;
                return new SyntheticSourceExample(in, out, this::mapping);
            }

            private final Set<String> previousValues = new HashSet<>();

            private Tuple<String, String> generateValue() {
                String v;
                if (previousValues.size() > 0 && randomBoolean()) {
                    v = randomFrom(previousValues);
                } else {
                    v = ESTestCase.randomAlphaOfLength(5);
                    previousValues.add(v);
                }
                return Tuple.tuple(v, v);
            }

            private void mapping(XContentBuilder b) throws IOException {
                minimalMapping(b);
            }

            @Override
            public List<SyntheticSourceInvalidExample> invalidExample() throws IOException {
                return List.of();
            }
        };
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }

    public void testDottedFieldNames() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("dotted.field");
            b.field("type", CountedKeywordFieldMapper.CONTENT_TYPE);
            b.endObject();
        }));
        ParsedDocument doc = mapper.parse(source(b -> b.field("dotted.field", "1234")));
        List<IndexableField> fields = doc.rootDoc().getFields("dotted.field");
        assertEquals(1, fields.size());
    }

    public void testDisableIndex() throws IOException {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", CountedKeywordFieldMapper.CONTENT_TYPE).field("index", false))
        );
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "1234")));
        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        assertEquals(IndexOptions.NONE, fields.get(0).fieldType().indexOptions());
        assertEquals(DocValuesType.SORTED_SET, fields.get(0).fieldType().docValuesType());
    }
}
