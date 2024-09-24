/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceProvider;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class PatchSourceMapperTests extends MapperServiceTestCase {
    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new TestPlugin());
    }

    public void testPatchSourceFlat() throws IOException {
        var mapperService = createMapperService(mapping(b -> {
            // field
            b.startObject("field");
            b.field("type", "patch");
            b.endObject();

            // another_field
            b.startObject("another_field");
            b.field("type", "keyword");
            b.endObject();
        }));
        assertSourcePatch(
            mapperService,
            Map.of("field", Map.of("obj", Map.of("key1", "value1")), "another_field", randomAlphaOfLengthBetween(5, 10)),
            true
        );
    }

    public void testPatchSourceFlatToCopyTo() throws IOException {
        var mapperService = createMapperService(mapping(b -> {
            // field
            b.startObject("field");
            b.field("type", "patch");
            b.field("copy_to", new String[] { "another_field" });
            b.endObject();

            // another_field
            b.startObject("another_field");
            b.field("type", "keyword");
            b.endObject();

            // another_field
            b.startObject("extra_field");
            b.field("type", "keyword");
            b.endObject();
        }));
        assertSourcePatch(mapperService, Map.of("field", "key1"), true);
    }

    public void testPatchSourceFlatFromCopyTo() throws IOException {
        var mapperService = createMapperService(mapping(b -> {
            // field
            b.startObject("field");
            b.field("type", "patch");
            b.endObject();

            // another_field
            b.startObject("another_field");
            b.field("type", "keyword");
            b.field("copy_to", new String[] { "field" });
            b.endObject();

            // another_field
            b.startObject("extra_field");
            b.field("type", "keyword");
            b.endObject();
        }));
        assertSourcePatch(mapperService, Map.of("another_field", "value1", "extra_field", "value2"), false);
    }

    public void testPatchSourceFlatMulti() throws IOException {
        var mapperService = createMapperService(mapping(b -> {
            // field
            b.startObject("field");
            b.field("type", "patch");
            b.endObject();

            // another_field
            b.startObject("another_field");
            b.field("type", "keyword");
            b.endObject();
        }));
        var exc = expectThrows(
            DocumentParsingException.class,
            () -> assertSourcePatch(
                mapperService,
                Map.of(
                    "field",
                    List.of(Map.of("obj", Map.of("key1", "value1")), Map.of("another", "one")),
                    "another_field",
                    randomAlphaOfLengthBetween(5, 10)
                ),
                true
            )
        );
        assertThat(exc.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(exc.getDetailedMessage(), containsString("[field] does not support patching multiple values"));
    }

    public void testPatchSourceObject() throws IOException {
        var mapperService = createMapperService(mapping(b -> {
            // obj
            b.startObject("obj");
            b.startObject("properties");

            // field
            b.startObject("field");
            b.field("type", "patch");
            b.endObject();

            // obj.another_field
            b.startObject("another_field");
            b.field("type", "keyword");
            b.endObject();

            b.endObject();
            b.endObject();

            // another_field
            b.startObject("another_field");
            b.field("type", "keyword");
            b.endObject();
        }));
        assertSourcePatch(
            mapperService,
            Map.of("obj", Map.of("field", Map.of("key1", "value1")), "another_field", randomAlphaOfLengthBetween(5, 10)),
            true
        );
    }

    public void testPatchSourceObjectFlat() throws IOException {
        var mapperService = createMapperService(mapping(b -> {
            // obj
            b.startObject("obj");
            b.startObject("properties");

            // obj.field
            b.startObject("field");
            b.field("type", "patch");
            b.endObject();

            // obj.another_field
            b.startObject("another_field");
            b.field("type", "keyword");
            b.endObject();

            b.endObject();
            b.endObject();

            // another_field
            b.startObject("another_field");
            b.field("type", "keyword");
            b.endObject();
        }));
        assertSourcePatch(
            mapperService,
            Map.of("obj.field", Map.of("key1", "value1"), "another_field", randomAlphaOfLengthBetween(5, 10)),
            true
        );
    }

    public void testPatchSourceNestedObject() throws IOException {
        var mapperService = createMapperService(mapping(b -> {
            // nested
            b.startObject("nested");
            b.field("type", "nested");
            b.startObject("properties");

            // nested.field
            b.startObject("field");
            b.field("type", "patch");
            b.endObject();

            // nested.another_field
            b.startObject("another_field");
            b.field("type", "keyword");
            b.endObject();

            b.endObject();
            b.endObject();

            // another_field
            b.startObject("another_field");
            b.field("type", "keyword");
            b.endObject();
        }));
        assertSourcePatch(
            mapperService,
            Map.of("nested", Map.of("field", Map.of("key1", "value1")), "another_field", randomAlphaOfLengthBetween(5, 10)),
            true
        );
    }

    public void testPatchSourceNestedArray() throws IOException {
        var mapperService = createMapperService(mapping(b -> {
            // nested
            b.startObject("nested");
            b.field("type", "nested");
            b.startObject("properties");

            // nested.field
            b.startObject("field");
            b.field("type", "patch");
            b.endObject();

            // nested.another_field
            b.startObject("another_field");
            b.field("type", "keyword");
            b.endObject();

            b.endObject();
            b.endObject();

            // another_field
            b.startObject("another_field");
            b.field("type", "keyword");
            b.endObject();
        }));
        assertSourcePatch(
            mapperService,
            Map.of(
                "nested",
                List.of(
                    Map.of("field", Map.of()),
                    Map.of(),
                    Map.of("field", Map.of("key1", "value1")),
                    Map.of("another_field", randomAlphaOfLengthBetween(5, 10))
                ),
                "another_field",
                randomAlphaOfLengthBetween(5, 10)
            ),
            true
        );
    }

    public void testPatchSourceMulti() throws IOException {
        var mapperService = createMapperService(mapping(b -> {
            // field
            b.startObject("field");
            b.field("type", "patch");
            b.endObject();

            // obj
            b.startObject("obj");
            b.startObject("properties");

            // obj.field
            b.startObject("field");
            b.field("type", "patch");
            b.endObject();

            // obj.another_field
            b.startObject("another_field");
            b.field("type", "keyword");
            b.endObject();

            b.endObject();
            b.endObject();

            // nested
            b.startObject("nested");
            b.field("type", "nested");
            b.startObject("properties");

            // nested.field
            b.startObject("field");
            b.field("type", "patch");
            b.endObject();

            // nested.another_field
            b.startObject("another_field");
            b.field("type", "keyword");
            b.endObject();

            b.endObject();
            b.endObject();

            // another_field
            b.startObject("another_field");
            b.field("type", "keyword");
            b.endObject();
        }));
        assertSourcePatch(
            mapperService,
            Map.of(
                "field",
                Map.of("obj", Map.of("key1", "value1")),
                "obj",
                Map.of("field", Map.of("key1", "value1")),
                "nested",
                Map.of("field", Map.of("key1", "value1")),
                "another_field",
                randomAlphaOfLengthBetween(5, 10)
            ),
            true
        );
    }

    public void testPatchSourceMultiFlat() throws IOException {
        var mapperService = createMapperService(mapping(b -> {
            // field
            b.startObject("field");
            b.field("type", "patch");
            b.endObject();

            // obj
            b.startObject("obj");
            b.startObject("properties");

            // obj.field
            b.startObject("field");
            b.field("type", "patch");
            b.endObject();

            b.endObject();
            b.endObject();

            // nested
            b.startObject("nested");
            b.field("type", "nested");
            b.startObject("properties");

            // nested.field
            b.startObject("field");
            b.field("type", "patch");
            b.endObject();

            // nested.another_field
            b.startObject("another_field");
            b.field("type", "keyword");
            b.endObject();

            b.endObject();
            b.endObject();

            // another_field
            b.startObject("another_field");
            b.field("type", "keyword");
            b.endObject();
        }));
        assertSourcePatch(
            mapperService,
            Map.of(
                "field",
                Map.of("obj", Map.of("key1", "value1")),
                "obj.field",
                Map.of("key1", "value1"),
                "nested.field",
                Map.of("key1", "value1"),
                "another_field",
                randomAlphaOfLengthBetween(5, 10)
            ),
            true
        );
    }

    public void testPatchSourceWithIncludes() throws IOException {
        var mapperService = createMapperService(mapping(b -> {
            // field
            b.startObject("field");
            b.field("type", "patch");
            b.endObject();

            // another_field
            b.startObject("another_field");
            b.field("type", "keyword");
            b.endObject();
        }));
        assertSourcePatch(
            mapperService,
            Map.of("field", Map.of("obj", Map.of("key1", "value1")), "another_field", randomAlphaOfLengthBetween(5, 10)),
            true
        );
    }

    public static void assertSourcePatch(MapperService mapperService, Map<String, Object> source, boolean needsPatching)
        throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.value(source);
        SourceToParse origSource = new SourceToParse("0", BytesReference.bytes(builder), builder.contentType());
        ParsedDocument doc = mapperService.documentMapper().parse(origSource);
        var storedSource = doc.rootDoc().getField(SourceFieldMapper.NAME).binaryValue();
        if (needsPatching) {
            assertThat(storedSource.length, lessThan(origSource.source().length()));
            assertFalse(storedSource.utf8ToString().equals(origSource.source().utf8ToString()));
        } else {
            assertThat(storedSource.utf8ToString(), equalTo(origSource.source().utf8ToString()));
        }
        withLuceneIndex(mapperService, iw -> iw.addDocuments(doc.docs()), ir -> {
            Source actual = SourceProvider.fromLookup(mapperService.mappingLookup(), mapperService.getMapperMetrics().sourceFieldMetrics())
                .getSource(ir.leaves().get(0), doc.docs().size() - 1);
            assertEquals(origSource.source().utf8ToString(), actual.internalSourceRef().utf8ToString());
        });
    }

    static class TestPlugin extends Plugin implements MapperPlugin {
        @Override
        public Map<String, Mapper.TypeParser> getMappers() {
            return Map.of(BinaryPatchSourceFieldMapper.CONTENT_TYPE, BinaryPatchSourceFieldMapper.PARSER);
        }
    }

    static class BinaryPatchSourceFieldMapper extends FieldMapper {
        static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));
        static final String CONTENT_TYPE = "patch";

        static class Builder extends FieldMapper.Builder {
            protected Builder(String name) {
                super(name);
            }

            @Override
            protected Parameter<?>[] getParameters() {
                return new Parameter<?>[0];
            }

            @Override
            public FieldMapper build(MapperBuilderContext context) {
                BinaryFieldMapper.Builder b = new BinaryFieldMapper.Builder(leafName(), false).docValues(true);
                return new BinaryPatchSourceFieldMapper(leafName(), b.build(context), builderParams(b, context));
            }
        }

        protected BinaryPatchSourceFieldMapper(String simpleName, BinaryFieldMapper delegate, BuilderParams builderParams) {
            super(simpleName, delegate.fieldType(), builderParams);
        }

        @Override
        protected void parseCreateField(DocumentParserContext context) throws IOException {
            if (context.isWithinCopyTo() == false) {
                context.addSourceFieldPatch(this, context.parser().getTokenLocation());
            }
            XContentBuilder b = XContentBuilder.builder(context.parser().contentType().xContent());
            b.copyCurrentStructure(context.parser());
            context.doc().add(new BinaryDocValuesField(fullPath(), BytesReference.bytes(b).toBytesRef()));
            context.parser().skipChildren();
        }

        @Override
        protected String contentType() {
            return CONTENT_TYPE;
        }

        @Override
        public Builder getMergeBuilder() {
            return new Builder(leafName());
        }

        @Override
        protected SyntheticSourceSupport syntheticSourceSupport() {
            var fieldLoader = new BinaryDocValuesSyntheticFieldLoader(fullPath()) {
                @Override
                protected void writeValue(XContentBuilder b, BytesRef value) throws IOException {
                    try (var stream = new BytesArray(value.utf8ToString()).streamInput()) {
                        b.rawField(leafName(), stream, b.contentType());
                    }
                }
            };
            return new SyntheticSourceSupport.Native(fieldLoader);
        }

        @Override
        protected SourceLoader.PatchFieldLoader patchFieldLoader() {
            return new SourceLoader.SyntheticPatchFieldLoader(syntheticSourceSupport().loader());
        }
    }
}
