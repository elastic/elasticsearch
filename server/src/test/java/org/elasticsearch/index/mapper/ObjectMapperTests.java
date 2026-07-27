/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.mapper.ObjectMapper.Dynamic;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class ObjectMapperTests extends MapperServiceTestCase {

    public void testDifferentInnerObjectTokenFailure() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(mapping(b -> {}));
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> defaultMapper.parse(new SourceToParse("1", new BytesArray("""
                {
                     "object": {
                       "array":[
                       {
                         "object": { "value": "value" }
                       },
                       {
                         "object":"value"
                       }
                       ]
                     },
                     "value":"value"
                   }""".indent(1)), XContentType.JSON))
        );
        assertThat(
            e.getMessage(),
            containsString("object mapping for [object.array.object] tried to parse field [object] as object, but found a concrete value")
        );
    }

    public void testEmptyArrayProperties() throws Exception {
        createMapperService(topMapping(b -> b.startArray("properties").endArray()));
    }

    public void testEmptyFieldsArrayMultiFields() throws Exception {
        createMapperService(mapping(b -> {
            b.startObject("name");
            b.field("type", "text");
            b.startArray("fields").endArray();
            b.endObject();
        }));
    }

    public void testFieldsArrayMultiFieldsShouldThrowException() {
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(mapping(b -> {
            b.startObject("name");
            {
                b.field("type", "text");
                b.startArray("fields");
                {
                    b.startObject().field("test", "string").endObject();
                    b.startObject().field("test2", "string").endObject();
                }
                b.endArray();
            }
            b.endObject();
        })));

        assertThat(e.getMessage(), containsString("expected map for property [fields]"));
        assertThat(e.getMessage(), containsString("but got a class java.util.ArrayList"));
    }

    public void testEmptyFieldsArray() throws Exception {
        createMapperService(mapping(b -> b.startArray("fields").endArray()));
    }

    public void testFieldsWithFilledArrayShouldThrowException() {
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(mapping(b -> {
            b.startArray("fields");
            {
                b.startObject().field("test", "string").endObject();
                b.startObject().field("test2", "string").endObject();
            }
            b.endArray();
        })));
        assertThat(e.getMessage(), containsString("Expected map for property [fields]"));
    }

    public void testFieldPropertiesArray() throws Exception {
        // TODO this isn't actually testing an array?
        createMapperService(mapping(b -> {
            b.startObject("name");
            {
                b.field("type", "text");
                b.startObject("fields");
                {
                    b.startObject("raw").field("type", "keyword").endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));
    }

    public void testMerge() throws IOException {
        MergeReason reason = randomFrom(MergeReason.values());
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "keyword")));
        DocumentMapper mapper = mapperService.documentMapper();
        assertNull(mapper.mapping().getRoot().dynamic());
        CompressedXContent mergeWith = new CompressedXContent(BytesReference.bytes(topMapping(b -> b.field("dynamic", "strict"))));
        Mapping merged = mapperService.mergeMappings(mergeWith, reason, Long.MAX_VALUE);
        assertEquals(Dynamic.STRICT, merged.getRoot().dynamic());
    }

    public void testMergeEnabledForIndexTemplates() throws IOException {
        MapperService mapperService = createSytheticSourceMapperService(mapping(b -> {}));
        merge(mapperService, MergeReason.INDEX_TEMPLATE, mapping(b -> {
            b.startObject("object");
            {
                b.field("type", "object");
                b.field("enabled", false);
            }
            b.endObject();
        }));

        DocumentMapper mapper = mapperService.documentMapper();
        assertNull(mapper.mapping().getRoot().dynamic());

        // If we don't explicitly set 'enabled', then the mapping should not change.
        String update = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("object")
                .field("type", "object")
                .field("dynamic", false)
                .endObject()
                .endObject()
                .endObject()
        );
        mapper = mapperService.merge("type", new CompressedXContent(update), MergeReason.INDEX_TEMPLATE);

        ObjectMapper objectMapper = mapper.mappers().objectMappers().get("object");
        assertNotNull(objectMapper);
        assertFalse(objectMapper.isEnabled());
        assertEquals(ObjectMapper.Subobjects.ENABLED, objectMapper.subobjects());
        assertTrue(objectMapper.sourceKeepMode().isEmpty());
    }

    public void testFieldReplacementForIndexTemplates() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {}));
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("object")
                .startObject("properties")
                .startObject("field1")
                .field("type", "keyword")
                .endObject()
                .startObject("field2")
                .field("type", "text")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        String update = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("object")
                .startObject("properties")
                .startObject("field2")
                .field("type", "integer")
                .endObject()
                .startObject("field3")
                .field("type", "text")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );
        DocumentMapper mapper = mapperService.merge(
            MapperService.SINGLE_MAPPING_NAME,
            List.of(new CompressedXContent(mapping), new CompressedXContent(update)),
            MergeReason.INDEX_TEMPLATE
        );

        String expected = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject(MapperService.SINGLE_MAPPING_NAME)
                .startObject("properties")
                .startObject("object")
                .startObject("properties")
                .startObject("field1")
                .field("type", "keyword")
                .endObject()
                .startObject("field2")
                .field("type", "integer")
                .endObject()
                .startObject("field3")
                .field("type", "text")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        assertEquals(expected, mapper.mappingSource().toString());
    }

    public void testDisallowFieldReplacementForIndexTemplates() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {}));
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("object")
                .startObject("properties")
                .startObject("field1")
                .field("type", "object")
                .endObject()
                .startObject("field2")
                .field("type", "text")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        String firstUpdate = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("object")
                .startObject("properties")
                .startObject("field2")
                .field("type", "nested")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        // We can only check such assertion in sequential merges. Bulk merges allow such type substitution as it replaces entire field
        // mapping subtrees
        mapperService.merge(MapperService.SINGLE_MAPPING_NAME, new CompressedXContent(mapping), MergeReason.INDEX_TEMPLATE);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> mapperService.merge(MapperService.SINGLE_MAPPING_NAME, new CompressedXContent(firstUpdate), MergeReason.INDEX_TEMPLATE)
        );
        assertThat(e.getMessage(), containsString("can't merge a non-nested mapping [object.field2] with a nested mapping"));

        String secondUpdate = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("object")
                .startObject("properties")
                .startObject("field1")
                .field("type", "text")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );
        e = expectThrows(
            IllegalArgumentException.class,
            () -> mapperService.merge(MapperService.SINGLE_MAPPING_NAME, new CompressedXContent(secondUpdate), MergeReason.INDEX_TEMPLATE)
        );
        assertThat(e.getMessage(), containsString("can't merge a non object mapping [object.field1] with an object mapping"));
    }

    public void testFieldReplacementSubobjectsFalse() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("obj").field("type", "object").field("subobjects", false).startObject("properties");
            {
                b.startObject("my.field").field("type", "keyword").endObject();
            }
            b.endObject().endObject();
        }));
        DocumentMapper mapper = mapperService.documentMapper();
        assertNull(mapper.mapping().getRoot().dynamic());
        CompressedXContent mergeWith = new CompressedXContent(BytesReference.bytes(topMapping(b -> {
            b.startObject("properties").startObject("obj").field("type", "object").field("subobjects", false).startObject("properties");
            {
                b.startObject("my.field").field("type", "long").endObject();
            }
            b.endObject().endObject().endObject();
        })));

        // Fails on mapping update.
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> mapperService.mergeMappings(mergeWith, MergeReason.MAPPING_UPDATE, Long.MAX_VALUE)
        );
        assertEquals("mapper [obj.my.field] cannot be changed from type [keyword] to [long]", exception.getMessage());

        // Passes on template merging.
        Mapping merged = mapperService.mergeMappings(mergeWith, MergeReason.INDEX_TEMPLATE, Long.MAX_VALUE);
        assertThat(((ObjectMapper) merged.getRoot().getMapper("obj")).getMapper("my.field"), instanceOf(NumberFieldMapper.class));
    }

    public void testUnknownLegacyFields() throws Exception {
        MapperService service = createMapperService(IndexVersion.fromId(5000099), Settings.EMPTY, () -> false, mapping(b -> {
            b.startObject("name");
            b.field("type", "unknown");
            b.field("unknown_setting", 5);
            b.endObject();
        }));
        assertThat(service.fieldType("name"), instanceOf(PlaceHolderFieldMapper.PlaceHolderFieldType.class));
    }

    public void testUnmappedLegacyFields() throws Exception {
        MapperService service = createMapperService(IndexVersion.fromId(5000099), Settings.EMPTY, () -> false, mapping(b -> {
            b.startObject("name");
            b.field("type", CompletionFieldMapper.CONTENT_TYPE);
            b.field("unknown_setting", 5);
            b.endObject();
        }));
        assertThat(service.fieldType("name"), instanceOf(PlaceHolderFieldMapper.PlaceHolderFieldType.class));
    }

    public void testSubobjectsFalse() throws Exception {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("metrics.service");
            {
                b.field("subobjects", false);
                b.startObject("properties");
                {
                    b.startObject("time");
                    b.field("type", "long");
                    b.endObject();
                    b.startObject("time.max");
                    b.field("type", "long");
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));
        assertNotNull(mapperService.fieldType("metrics.service.time"));
        assertNotNull(mapperService.fieldType("metrics.service.time.max"));
    }

    public void testSubobjectsFalseWithInnerObject() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("metrics.service");
            {
                b.field("subobjects", false);
                b.startObject("properties");
                {
                    b.startObject("time");
                    {
                        b.startObject("properties");
                        {
                            b.startObject("max");
                            b.field("type", "long");
                            b.endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));
        assertNull(mapperService.fieldType("metrics.service.time"));
        assertNotNull(mapperService.fieldType("metrics.service.time.max"));
    }

    public void testSubobjectsFalseWithInnerNested() throws IOException {
        // A nested field is accepted under subobjects:false: it is a hierarchical document boundary rather than a
        // plain object, so it is kept rather than flattened away.
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("metrics.service");
            {
                b.field("subobjects", false);
                b.startObject("properties");
                {
                    b.startObject("time");
                    {
                        b.field("type", "nested");
                        b.startObject("properties");
                        b.startObject("max").field("type", "long").endObject();
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));
        assertThat(mapperService.mappingLookup().objectMappers().get("metrics.service.time"), instanceOf(NestedObjectMapper.class));
        assertNotNull(mapperService.fieldType("metrics.service.time.max"));
    }

    public void testSubobjectsFalseRoot() throws Exception {
        MapperService mapperService = createMapperService(mappingNoSubobjects(b -> {
            b.startObject("metrics.service.time");
            b.field("type", "long");
            b.endObject();
            b.startObject("metrics.service.time.max");
            b.field("type", "long");
            b.endObject();
        }));
        assertNotNull(mapperService.fieldType("metrics.service.time"));
        assertNotNull(mapperService.fieldType("metrics.service.time.max"));
    }

    public void testExplicitDefaultSubobjects() throws Exception {
        MapperService mapperService = createMapperService(topMapping(b -> b.field("subobjects", true)));
        assertEquals("{\"_doc\":{\"subobjects\":true}}", Strings.toString(mapperService.mappingLookup().getMapping()));
    }

    public void testSubobjectsFalseRootWithInnerObject() throws IOException {
        MapperService mapperService = createMapperService(mappingNoSubobjects(b -> {
            b.startObject("metrics.service.time");
            {
                b.startObject("properties");
                {
                    b.startObject("max");
                    b.field("type", "long");
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));
        assertNull(mapperService.fieldType("metrics.service.time"));
        assertNotNull(mapperService.fieldType("metrics.service.time.max"));
    }

    public void testSubobjectsFalseRootWithInnerNested() throws IOException {
        // A nested field declared at a subobjects:false root is accepted and kept as a nested mapper.
        MapperService mapperService = createMapperService(mappingNoSubobjects(b -> {
            b.startObject("metrics.service");
            {
                b.field("type", "nested");
                b.startObject("properties");
                b.startObject("time").field("type", "long").endObject();
                b.endObject();
            }
            b.endObject();
        }));
        assertThat(mapperService.mappingLookup().objectMappers().get("metrics.service"), instanceOf(NestedObjectMapper.class));
        assertNotNull(mapperService.fieldType("metrics.service.time"));
    }

    public void testSubobjectsFalseNestedSiblingFlatFieldConflict() {
        // A flat dotted field [foo.baz] cannot exist as a sibling of nested field [foo]: it would be ambiguous
        // whether it belongs to the nested document or is a standalone field, so the mapping is rejected.
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(mappingNoSubobjects(b -> {
            b.startObject("foo");
            {
                b.field("type", "nested");
                b.startObject("properties");
                b.startObject("bar").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
            b.startObject("foo.baz").field("type", "keyword").endObject();
        })));
        assertThat(
            e.getMessage(),
            containsString("Field [foo.baz] cannot be added because [foo] is a nested field; its sub-fields must be declared")
        );
    }

    public void testSubobjectsFalseNestedWithinNested() throws IOException {
        // Nested-within-nested is expressed by declaring the inner nested field inside the outer's [properties].
        // Both 'foo' and 'foo.bar' become nested mappers.
        MapperService mapperService = createMapperService(mappingNoSubobjects(b -> {
            b.startObject("foo");
            {
                b.field("type", "nested");
                b.startObject("properties");
                {
                    b.startObject("bar");
                    {
                        b.field("type", "nested");
                        b.startObject("properties");
                        b.startObject("baz").field("type", "keyword").endObject();
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));
        assertThat(mapperService.mappingLookup().objectMappers().get("foo"), instanceOf(NestedObjectMapper.class));
        assertThat(mapperService.mappingLookup().objectMappers().get("foo.bar"), instanceOf(NestedObjectMapper.class));
        assertEquals(2, mapperService.mappingLookup().nestedLookup().getNestedMappers().size());
        assertNotNull(mapperService.fieldType("foo.bar.baz"));
    }

    public void testSubobjectsFalseAllowsDeeperNestedLevels() throws IOException {
        // Outside columnar, subobjects:false allows nested at any depth - the single-level restriction is columnar-only
        // (see testStrictColumnarModesRejectNestedWithinNested). Depth here is bounded only by the nested-fields limit.
        MapperService mapperService = createMapperService(mappingNoSubobjects(b -> {
            b.startObject("a");
            {
                b.field("type", "nested");
                b.startObject("properties");
                {
                    b.startObject("b");
                    {
                        b.field("type", "nested");
                        b.startObject("properties");
                        {
                            b.startObject("c");
                            {
                                b.field("type", "nested");
                                b.startObject("properties");
                                b.startObject("d").field("type", "keyword").endObject();
                                b.endObject();
                            }
                            b.endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));
        assertThat(mapperService.mappingLookup().objectMappers().get("a"), instanceOf(NestedObjectMapper.class));
        assertThat(mapperService.mappingLookup().objectMappers().get("a.b"), instanceOf(NestedObjectMapper.class));
        assertThat(mapperService.mappingLookup().objectMappers().get("a.b.c"), instanceOf(NestedObjectMapper.class));
        assertEquals(3, mapperService.mappingLookup().nestedLookup().getNestedMappers().size());
        assertNotNull(mapperService.fieldType("a.b.c.d"));
    }

    public void testSubobjectsFalseNestedProducesChildDocuments() throws IOException {
        // The runtime path under subobjects:false must still create one Lucene child document per nested value.
        DocumentMapper mapper = createMapperService(mappingNoSubobjects(b -> {
            b.startObject("comments");
            {
                b.field("type", "nested");
                b.startObject("properties");
                b.startObject("message").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
        })).documentMapper();
        ParsedDocument doc = mapper.parse(source(b -> {
            b.startArray("comments");
            b.startObject().field("message", "a").endObject();
            b.startObject().field("message", "b").endObject();
            b.endArray();
        }));
        // two nested child documents plus the root document
        assertEquals(3, doc.docs().size());
    }

    public void testSubobjectsCannotBeUpdated() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "object")));
        DocumentMapper mapper = mapperService.documentMapper();
        assertNull(mapper.mapping().getRoot().dynamic());
        CompressedXContent mergeWith = new CompressedXContent(BytesReference.bytes(fieldMapping(b -> {
            b.field("type", "object");
            b.field("subobjects", "false");
        })));
        MapperException exception = expectThrows(
            MapperException.class,
            () -> mapperService.mergeMappings(mergeWith, MergeReason.MAPPING_UPDATE, Long.MAX_VALUE)
        );
        assertEquals("the [subobjects] parameter can't be updated for the object mapping [field]", exception.getMessage());
    }

    public void testSubobjectsCannotBeUpdatedOnRoot() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> b.field("subobjects", false)));
        DocumentMapper mapper = mapperService.documentMapper();
        assertNull(mapper.mapping().getRoot().dynamic());
        CompressedXContent mergeWith = new CompressedXContent(BytesReference.bytes(topMapping(b -> { b.field("subobjects", true); })));
        MapperException exception = expectThrows(
            MapperException.class,
            () -> mapperService.mergeMappings(mergeWith, MergeReason.MAPPING_UPDATE, Long.MAX_VALUE)
        );
        assertEquals("the [subobjects] parameter can't be updated for the object mapping [_doc]", exception.getMessage());
    }

    /**
     * Makes sure that an empty object mapper returns {@code null} from
     * {@link SourceLoader.SyntheticFieldLoader#docValuesLoader}. This
     * is important because it allows us to skip whole chains of empty
     * fields when loading synthetic {@code _source}.
     */
    public void testSyntheticSourceDocValuesEmpty() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> b.startObject("o").field("type", "object").endObject()));
        ObjectMapper o = (ObjectMapper) mapper.mapping().getRoot().getMapper("o");
        assertThat(o.syntheticFieldLoader(null).docValuesLoader(null, null), nullValue());
        assertThat(mapper.mapping().getRoot().syntheticFieldLoader(null).docValuesLoader(null, null), nullValue());
    }

    /**
     * Makes sure that an object mapper containing only fields without
     * doc values returns {@code null} from
     * {@link SourceLoader.SyntheticFieldLoader#docValuesLoader}. This
     * is important because it allows us to skip whole chains of empty
     * fields when loading synthetic {@code _source}.
     */
    public void testSyntheticSourceDocValuesFieldWithout() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("o").startObject("properties");
            {
                b.startObject("kwd");
                {
                    b.field("type", "keyword");
                    b.field("doc_values", false);
                    b.field("store", true);
                }
                b.endObject();
            }
            b.endObject().endObject();
        }));
        ObjectMapper o = (ObjectMapper) mapper.mapping().getRoot().getMapper("o");
        assertThat(o.syntheticFieldLoader(null).docValuesLoader(null, null), nullValue());
        assertThat(mapper.mapping().getRoot().syntheticFieldLoader(null).docValuesLoader(null, null), nullValue());
    }

    public void testIgnoredSourceAlwaysLoadedRegardlessOfSourceFilter() throws IOException {
        MapperService mapperService = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("kwd").field("type", "keyword").field("ignore_above", 1).endObject();
            b.startObject("other").field("type", "keyword").field("ignore_above", 1).endObject();
        }));
        DocumentMapper mapper = mapperService.documentMapper();

        String unfilteredSource = syntheticSource(mapper, b -> b.field("kwd", "toolong").field("other", "alsotoolong"));
        assertEquals("{\"kwd\":\"toolong\",\"other\":\"alsotoolong\"}", unfilteredSource);

        SourceFilter filterKwdOnly = new SourceFilter(new String[] { "kwd" }, null);
        String filteredSource = syntheticSource(mapper, filterKwdOnly, b -> b.field("kwd", "toolong").field("other", "alsotoolong"));
        assertEquals("{\"kwd\":\"toolong\"}", filteredSource);
    }

    public void testStoreArraySourceInSyntheticSourceMode() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("o").field("type", "object").field("synthetic_source_keep", "arrays").endObject();
        })).documentMapper();
        assertNotNull(mapper.mapping().getRoot().getMapper("o"));
    }

    public void testStoreArraySourceNoopInNonSyntheticSourceMode() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("o").field("type", "object").field("synthetic_source_keep", "arrays").endObject();
        }));
        assertNotNull(mapper.mapping().getRoot().getMapper("o"));
    }

    public void testNestedObjectWithMultiFieldsgetTotalFieldsCount() {
        ObjectMapper.Builder mapperBuilder = new ObjectMapper.Builder("parent_size_1").add(
            new ObjectMapper.Builder("child_size_2").add(
                new TextFieldMapper.Builder("grand_child_size_3", defaultIndexSettings(), createDefaultIndexAnalyzers(), false)
                    .addMultiField(new KeywordFieldMapper.Builder("multi_field_size_4", defaultIndexSettings()))
                    .addMultiField(
                        new TextFieldMapper.Builder("grand_child_size_5", defaultIndexSettings(), createDefaultIndexAnalyzers(), true)
                            .addMultiField(
                                new KeywordFieldMapper.Builder("multi_field_of_multi_field_size_6", defaultIndexSettings(), true)
                            )
                    )
            )
        );
        assertThat(mapperBuilder.build(MapperBuilderContext.root(false, false)).getTotalFieldsCount(), equalTo(6));
    }

    public void testWithoutMappers() throws IOException {
        ObjectMapper shallowObject = createObjectMapperWithAllParametersSet(b -> {});
        ObjectMapper object = createObjectMapperWithAllParametersSet(b -> {
            b.startObject("keyword");
            {
                b.field("type", "keyword");
            }
            b.endObject();
        });
        assertThat(object.withoutMappers().toString(), equalTo(shallowObject.toString()));
    }

    private ObjectMapper createObjectMapperWithAllParametersSet(CheckedConsumer<XContentBuilder, IOException> propertiesBuilder)
        throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("object");
            {
                b.field("type", "object");
                b.field("subobjects", false);
                b.field("enabled", false);
                b.field("dynamic", false);
                b.field("synthetic_source_keep", "arrays");
                b.startObject("properties");
                propertiesBuilder.accept(b);
                b.endObject();
            }
            b.endObject();
        })).documentMapper();
        return (ObjectMapper) mapper.mapping().getRoot().getMapper("object");
    }

    public void testFlatten() {
        MapperBuilderContext rootContext = MapperBuilderContext.root(false, false);
        ObjectMapper objectMapper = new ObjectMapper.Builder("parent", Explicit.of(ObjectMapper.Subobjects.DISABLED)).add(
            new ObjectMapper.Builder("child").add(new KeywordFieldMapper.Builder("keyword2", defaultIndexSettings()))
        ).add(new KeywordFieldMapper.Builder("keyword1", defaultIndexSettings())).build(rootContext);
        List<String> fields = objectMapper.mappers.values().stream().map(Mapper::fullPath).toList();
        assertThat(fields, containsInAnyOrder("parent.keyword1", "parent.child.keyword2"));
    }

    public void testFlattenDynamicIncompatible() {
        MapperBuilderContext rootContext = MapperBuilderContext.root(false, false);
        ObjectMapper.Builder parentBuilder = new ObjectMapper.Builder("parent", Explicit.of(ObjectMapper.Subobjects.DISABLED)).add(
            new ObjectMapper.Builder("child").dynamic(Dynamic.FALSE)
        );

        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> parentBuilder.build(rootContext));
        assertEquals(
            "Object mapper [parent.child] was found in a context where subobjects is set to false. "
                + "Auto-flattening [parent.child] failed because the value of [dynamic] (FALSE) is not compatible with "
                + "the value from its parent context (TRUE)",
            exception.getMessage()
        );
    }

    public void testFlattenEnabledFalse() {
        MapperBuilderContext rootContext = MapperBuilderContext.root(false, false);
        ObjectMapper.Builder parentBuilder = new ObjectMapper.Builder("parent", Explicit.of(ObjectMapper.Subobjects.DISABLED)).add(
            new ObjectMapper.Builder("child").enabled(false)
        );

        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> parentBuilder.build(rootContext));
        assertEquals(
            "Object mapper [parent.child] was found in a context where subobjects is set to false. "
                + "Auto-flattening [parent.child] failed because the value of [enabled] is [false];"
                + " no fields with the prefix [parent.child] are allowed",
            exception.getMessage()
        );
    }

    public void testFlattenExplicitSubobjectsTrue() {
        MapperBuilderContext rootContext = MapperBuilderContext.root(false, false);
        ObjectMapper.Builder parentBuilder = new ObjectMapper.Builder("parent", Explicit.of(ObjectMapper.Subobjects.DISABLED)).add(
            new ObjectMapper.Builder("child", Explicit.of(ObjectMapper.Subobjects.ENABLED))
        );

        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> parentBuilder.build(rootContext));
        assertEquals(
            "Object mapper [parent.child] was found in a context where subobjects is set to false. "
                + "Auto-flattening [parent.child] failed because the value of [subobjects] is [true]",
            exception.getMessage()
        );
    }

    public void testFindParentMapper() {
        MapperBuilderContext rootContext = MapperBuilderContext.root(false, false);

        var rootBuilder = new RootObjectMapper.Builder("_doc");
        rootBuilder.add(new KeywordFieldMapper.Builder("keyword", defaultIndexSettings()));

        var child = new ObjectMapper.Builder("child");
        child.add(new KeywordFieldMapper.Builder("keyword2", defaultIndexSettings()));
        child.add(new KeywordFieldMapper.Builder("keyword.with.dot", defaultIndexSettings()));
        var secondLevelChild = new ObjectMapper.Builder("child2");
        secondLevelChild.add(new KeywordFieldMapper.Builder("keyword22", defaultIndexSettings()));
        child.add(secondLevelChild);
        rootBuilder.add(child);

        var childWithDot = new ObjectMapper.Builder("childwith.dot");
        childWithDot.add(new KeywordFieldMapper.Builder("keyword3", defaultIndexSettings()));
        childWithDot.add(new KeywordFieldMapper.Builder("keyword4.with.dot", defaultIndexSettings()));
        rootBuilder.add(childWithDot);

        RootObjectMapper root = rootBuilder.build(rootContext);

        assertEquals("_doc", root.findParentMapper("keyword").fullPath());
        assertNull(root.findParentMapper("aa"));

        assertEquals("child", root.findParentMapper("child.keyword2").fullPath());
        assertEquals("child", root.findParentMapper("child.keyword.with.dot").fullPath());
        assertNull(root.findParentMapper("child.long"));
        assertNull(root.findParentMapper("child.long.hello"));
        assertEquals("child.child2", root.findParentMapper("child.child2.keyword22").fullPath());

        assertEquals("childwith.dot", root.findParentMapper("childwith.dot.keyword3").fullPath());
        assertEquals("childwith.dot", root.findParentMapper("childwith.dot.keyword4.with.dot").fullPath());
        assertNull(root.findParentMapper("childwith.dot.long"));
        assertNull(root.findParentMapper("childwith.dot.long.hello"));
    }

    public void testStrictColumnarModesAutoFlattenSubobjects() throws Exception {
        for (IndexMode indexMode : List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR)) {
            Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), indexMode.getName()).build();
            MapperService mapperService = createMapperService(settings, mapping(b -> {
                b.startObject("metrics");
                {
                    b.startObject("properties");
                    {
                        b.startObject("time").field("type", "long").endObject();
                        b.startObject("count").field("type", "long").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }));
            assertEquals(ObjectMapper.Subobjects.DISABLED, mapperService.documentMapper().mapping().getRoot().subobjects());
            assertNotNull(mapperService.fieldType("metrics.time"));
            assertNotNull(mapperService.fieldType("metrics.count"));
            assertNull(mapperService.mappingLookup().objectMappers().get("metrics"));
        }
    }

    public void testStrictColumnarModesRejectSubobjectsTrueOnObjectField() {
        for (IndexMode indexMode : List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR)) {
            Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), indexMode.getName()).build();
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(settings, mapping(b -> {
                b.startObject("metrics");
                {
                    b.field("subobjects", true);
                    b.startObject("properties");
                    b.startObject("time").field("type", "long").endObject();
                    b.endObject();
                }
                b.endObject();
            })));
            assertThat(e.getMessage(), containsString("subobjects [true] is not supported in columnar mode"));
        }
    }

    public void testStrictColumnarModesAcceptSubobjectsFalseOnObjectFieldAsNoOp() throws Exception {
        for (IndexMode indexMode : List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR)) {
            final Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), indexMode.getName()).build();
            final MapperService mapperService = createMapperService(settings, mapping(b -> {
                b.startObject("metrics");
                {
                    b.field("subobjects", false);
                    b.startObject("properties");
                    b.startObject("time").field("type", "long").endObject();
                    b.endObject();
                }
                b.endObject();
            }));
            assertEquals(ObjectMapper.Subobjects.DISABLED, mapperService.documentMapper().mapping().getRoot().subobjects());
            assertNotNull(mapperService.fieldType("metrics.time"));
            assertNull(mapperService.mappingLookup().objectMappers().get("metrics"));
        }
    }

    public void testStrictColumnarModesRejectRuntimeDynamic() {
        for (IndexMode indexMode : List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR)) {
            Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), indexMode.getName()).build();
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(settings, mapping(b -> {
                b.startObject("metrics");
                {
                    b.field("dynamic", "runtime");
                    b.startObject("properties");
                    b.startObject("time").field("type", "long").endObject();
                    b.endObject();
                }
                b.endObject();
            })));
            assertThat(e.getMessage(), containsString("dynamic [runtime] is not supported in strict columnar mode"));
        }
    }

    public void testColumnarModesRejectSyntheticSourceKeepOnObject() {
        for (IndexMode indexMode : List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR)) {
            for (String value : List.of("all", "arrays", "none")) {
                Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), indexMode.getName()).build();
                MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(settings, mapping(b -> {
                    b.startObject("metrics");
                    {
                        b.field(Mapper.SYNTHETIC_SOURCE_KEEP_PARAM, value);
                        b.startObject("properties");
                        b.startObject("time").field("type", "long").endObject();
                        b.endObject();
                    }
                    b.endObject();
                })));
                assertThat(
                    e.getMessage(),
                    containsString(
                        "parameter ["
                            + Mapper.SYNTHETIC_SOURCE_KEEP_PARAM
                            + "] is not allowed on object [metrics] in index using ["
                            + indexMode
                            + "] index mode"
                    )
                );
            }
        }
    }

    public void testColumnarModesRejectStoreArraySourceOnObject() {
        for (IndexMode indexMode : List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR)) {
            Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), indexMode.getName()).build();
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(settings, mapping(b -> {
                b.startObject("metrics");
                {
                    b.field("store_array_source", true);
                    b.startObject("properties");
                    b.startObject("time").field("type", "long").endObject();
                    b.endObject();
                }
                b.endObject();
            })));
            assertThat(
                e.getMessage(),
                containsString(
                    "parameter [store_array_source] is not allowed on object [metrics] in index using [" + indexMode + "] index mode"
                )
            );
        }
    }

    public void testStrictColumnarModesAllowNested() throws Exception {
        for (IndexMode indexMode : List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR)) {
            Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), indexMode.getName()).build();
            MapperService mapperService = createMapperService(settings, mapping(b -> {
                b.startObject("comments");
                {
                    b.field("type", "nested");
                    b.startObject("properties");
                    {
                        b.startObject("message").field("type", "keyword").endObject();
                        b.startObject("votes").field("type", "long").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }));
            // The nested field survives subobjects:false as a genuine document boundary.
            Mapper comments = mapperService.mappingLookup().objectMappers().get("comments");
            assertThat(comments, instanceOf(NestedObjectMapper.class));
            assertTrue(mapperService.mappingLookup().nestedLookup().getNestedMappers().containsKey("comments"));
            // Its leaf children are mapped under the nested path.
            assertNotNull(mapperService.fieldType("comments.message"));
            assertNotNull(mapperService.fieldType("comments.votes"));
        }
    }

    public void testStrictColumnarModesRejectNestedWithinNested() {
        for (IndexMode indexMode : List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR)) {
            Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), indexMode.getName()).build();
            // Columnar supports a single level of nesting only; subobjects:false still allows multiple levels
            // (see testSubobjectsFalseNestedWithinNested).
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(settings, mapping(b -> {
                b.startObject("comments");
                {
                    b.field("type", "nested");
                    b.startObject("properties");
                    {
                        b.startObject("replies");
                        {
                            b.field("type", "nested");
                            b.startObject("properties");
                            b.startObject("text").field("type", "keyword").endObject();
                            b.endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            })));
            assertThat(e.getMessage(), containsString("columnar index modes support only a single level of nesting"));
        }
    }

    public void testStrictColumnarModesRejectEnabledFalseInsideNested() {
        for (IndexMode indexMode : List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR)) {
            Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), indexMode.getName()).build();
            // Columnar's per-prefix flattening facets (enabled:false, heterogeneous dynamic) are captured on the root
            // object only. Inside a nested scope that capture would be lost, so they are rejected rather than silently
            // mishandled - the single nested level stays plain (flat leaves, no per-prefix facets), which is simpler to
            // reason about. enabled:false is accepted at the root (see testStrictColumnarModesAllowEnabledFalse).
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(settings, mapping(b -> {
                b.startObject("comments");
                {
                    b.field("type", "nested");
                    b.startObject("properties");
                    {
                        b.startObject("meta");
                        {
                            b.field("enabled", false);
                            b.startObject("properties");
                            b.startObject("host").field("type", "keyword").endObject();
                            b.endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            })));
            assertThat(e.getMessage(), containsString("the value of [enabled] is [false]"));
        }
    }

    public void testStrictColumnarModesRejectMismatchedDynamicInsideNested() {
        for (IndexMode indexMode : List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR)) {
            Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), indexMode.getName()).build();
            // Same boundary for a per-prefix dynamic override: accepted at the root
            // (testStrictColumnarModesAllowHeterogeneousDynamic), rejected one level inside a nested field.
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(settings, mapping(b -> {
                b.startObject("comments");
                {
                    b.field("type", "nested");
                    b.field("dynamic", "true");
                    b.startObject("properties");
                    {
                        b.startObject("meta");
                        {
                            b.field("dynamic", "false");
                            b.startObject("properties");
                            b.startObject("x").field("type", "keyword").endObject();
                            b.endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            })));
            assertThat(e.getMessage(), containsString("the value of [dynamic]"));
        }
    }

    public void testStrictColumnarModesNestedInsideFlattenedObject() throws Exception {
        for (IndexMode indexMode : List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR)) {
            Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), indexMode.getName()).build();
            MapperService mapperService = createMapperService(settings, mapping(b -> {
                b.startObject("meta");
                {
                    b.startObject("properties");
                    {
                        b.startObject("host").field("type", "keyword").endObject();
                        b.startObject("comments");
                        {
                            b.field("type", "nested");
                            b.startObject("properties");
                            b.startObject("message").field("type", "keyword").endObject();
                            b.endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }));
            // The plain object 'meta' is flattened away, but the nested field within it is preserved
            // as a boundary under its full dotted path.
            assertNull(mapperService.mappingLookup().objectMappers().get("meta"));
            assertNotNull(mapperService.fieldType("meta.host"));
            Mapper comments = mapperService.mappingLookup().objectMappers().get("meta.comments");
            assertThat(comments, instanceOf(NestedObjectMapper.class));
            assertTrue(mapperService.mappingLookup().nestedLookup().getNestedMappers().containsKey("meta.comments"));
            assertNotNull(mapperService.fieldType("meta.comments.message"));
        }
    }

    public void testStrictColumnarModesNestedInteriorIsFlat() throws Exception {
        for (IndexMode indexMode : List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR)) {
            Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), indexMode.getName()).build();
            MapperService mapperService = createMapperService(settings, mapping(b -> {
                b.startObject("comments");
                {
                    b.field("type", "nested");
                    b.startObject("properties");
                    {
                        b.startObject("author");
                        {
                            b.startObject("properties");
                            b.startObject("name").field("type", "keyword").endObject();
                            b.endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }));
            // The nested field inherits columnar's subobjects:false, so a plain sub-object inside it is flattened away
            // (only the nested boundary itself remains hierarchical).
            assertThat(mapperService.mappingLookup().objectMappers().get("comments"), instanceOf(NestedObjectMapper.class));
            assertNull(mapperService.mappingLookup().objectMappers().get("comments.author"));
            assertNotNull(mapperService.fieldType("comments.author.name"));
        }
    }

    public void testStrictColumnarModesFieldResolutionFlatFirstThenNested() throws Exception {
        for (IndexMode indexMode : List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR)) {
            Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), indexMode.getName()).build();
            MapperService mapperService = createMapperService(settings, mapping(b -> {
                b.startObject("host.name").field("type", "keyword").endObject();
                b.startObject("comments");
                {
                    b.field("type", "nested");
                    b.startObject("properties");
                    b.startObject("author.name").field("type", "keyword").endObject();
                    b.endObject();
                }
                b.endObject();
            }));
            NestedLookup nestedLookup = mapperService.mappingLookup().nestedLookup();
            // A non-nested field resolves as a flat leaf from the root and has no nested parent - identical to
            // plain subobjects:false resolution.
            assertNotNull(mapperService.fieldType("host.name"));
            assertNull(nestedLookup.getNestedParent("host.name"));
            // A field inside a nested document still resolves by its full path, and its nested scope is found by
            // matching the longest nested prefix ("from the end").
            assertNotNull(mapperService.fieldType("comments.author.name"));
            assertEquals("comments", nestedLookup.getNestedParent("comments.author.name"));
        }
    }

    public void testStrictColumnarModesNestedSyntheticSourceRoundTrip() throws Exception {
        // Scoped to COLUMNAR: LOGSDB_COLUMNAR's default mapping requires a @timestamp value on every document.
        // The synthetic source reconstruction path is identical for both modes (both are strict columnar and default
        // to synthetic source); the mapping-acceptance tests above already cover LOGSDB_COLUMNAR.
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
        DocumentMapper mapper = createMapperService(settings, mapping(b -> {
            b.startObject("comments");
            {
                b.field("type", "nested");
                b.startObject("properties");
                {
                    b.startObject("message").field("type", "keyword").endObject();
                    b.startObject("votes").field("type", "long").endObject();
                }
                b.endObject();
            }
            b.endObject();
        })).documentMapper();

        // Each nested object is indexed as its own child document; the nested synthetic source loader
        // reconstructs the array in document order from the children's doc values.
        String synthetic = syntheticSource(mapper, b -> {
            b.startArray("comments");
            {
                b.startObject().field("message", "first").field("votes", 3).endObject();
                b.startObject().field("message", "second").field("votes", 7).endObject();
            }
            b.endArray();
        });
        assertThat(synthetic, equalTo("""
            {"comments":[{"message":"first","votes":3},{"message":"second","votes":7}]}"""));
    }

    public void testStrictColumnarModesDynamicFieldInsideNestedConverges() throws Exception {
        // A nested object is a real document boundary even under the columnar subobjects:false default, so a dynamic field inside it
        // must be added inside the nested mapper ([n.ndyn]). If it were flattened to a root leaf ([ndyn]), the proposed mapping update
        // would target the wrong path and re-parsing the same document would keep proposing it, tripping the indexing-time
        // noop-mapping-update infinite-retry guard (BulkPrimaryExecutionContext#resetForNoopMappingUpdateRetry).
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
        MapperService ms = createMapperService(settings, mapping(b -> {
            b.startObject("n").field("type", "nested");
            b.startObject("properties").startObject("leaf").field("type", "keyword").endObject().endObject();
            b.endObject();
        }));
        CheckedConsumer<XContentBuilder, IOException> docB = b -> {
            b.startArray("n");
            b.startObject().field("leaf", "y").field("ndyn", "p").endObject();
            b.endArray();
        };
        ParsedDocument doc = ms.documentMapper().parse(source(docB));
        CompressedXContent update = doc.dynamicMappingsUpdate();
        assertNotNull("first parse proposes a dynamic update", update);
        mergeDynamicUpdate(ms, update);
        // the dynamic field landed inside the nested object, not as a flat field at the root
        assertNotNull("dynamic field must be mapped inside the nested object", ms.mappingLookup().getMapper("n.ndyn"));
        assertNull("dynamic field must not be mapped at the root", ms.mappingLookup().getMapper("ndyn"));
        ParsedDocument doc2 = ms.documentMapper().parse(source(docB));
        assertNull("dynamic mapping must converge (no infinite noop-update loop)", doc2.dynamicMappingsUpdate());
    }

    public void testStrictColumnarModesDynamicFieldInsideNestedInObjectConverges() throws Exception {
        // Same as above but the nested object is declared inside a (flattened) object, so its full path is [obj.n]: the dynamic
        // field must still land inside the nested mapper ([obj.n.ndyn]) rather than as a flat leaf, so the mapping converges.
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
        MapperService ms = createMapperService(settings, mapping(b -> {
            b.startObject("obj");
            b.startObject("properties");
            b.startObject("n").field("type", "nested");
            b.startObject("properties").startObject("leaf").field("type", "keyword").endObject().endObject();
            b.endObject();
            b.endObject();
            b.endObject();
        }));
        CheckedConsumer<XContentBuilder, IOException> docB = b -> {
            b.startObject("obj");
            b.startArray("n");
            b.startObject().field("leaf", "y").field("ndyn", "p").endObject();
            b.endArray();
            b.endObject();
        };
        ParsedDocument doc = ms.documentMapper().parse(source(docB));
        CompressedXContent update = doc.dynamicMappingsUpdate();
        assertNotNull("first parse proposes a dynamic update", update);
        mergeDynamicUpdate(ms, update);
        assertNotNull("dynamic field must be mapped inside the nested object", ms.mappingLookup().getMapper("obj.n.ndyn"));
        ParsedDocument doc2 = ms.documentMapper().parse(source(docB));
        assertNull("dynamic mapping must converge (no infinite noop-update loop)", doc2.dynamicMappingsUpdate());
    }

    public void testDynamicFieldInsideNestedInsideNestedConverges() throws Exception {
        // Outside columnar, multiple nested levels are allowed under subobjects:false. A dynamic field at the deepest level must
        // descend through both nested boundaries and be mapped inside the innermost nested ([n1.n2.ndyn]). This exercises the
        // recursive descent of addDynamic and the nested-ancestor lookup at depth (a non-columnar index, so no feature flag).
        MapperService ms = createMapperService(topMapping(b -> {
            b.field("subobjects", false);
            b.startObject("properties");
            b.startObject("n1").field("type", "nested").field("subobjects", false);
            b.startObject("properties");
            b.startObject("n2").field("type", "nested");
            b.startObject("properties").startObject("leaf").field("type", "keyword").endObject().endObject();
            b.endObject();
            b.endObject();
            b.endObject();
            b.endObject();
        }));
        CheckedConsumer<XContentBuilder, IOException> docB = b -> {
            b.startObject("n1");
            b.startArray("n2");
            b.startObject().field("leaf", "y").field("ndyn", "p").endObject();
            b.endArray();
            b.endObject();
        };
        ParsedDocument doc = ms.documentMapper().parse(source(docB));
        CompressedXContent update = doc.dynamicMappingsUpdate();
        assertNotNull("first parse proposes a dynamic update", update);
        mergeDynamicUpdate(ms, update);
        assertNotNull("dynamic field must be mapped inside the innermost nested object", ms.mappingLookup().getMapper("n1.n2.ndyn"));
        ParsedDocument doc2 = ms.documentMapper().parse(source(docB));
        assertNull("dynamic mapping must converge (no infinite noop-update loop)", doc2.dynamicMappingsUpdate());
    }

    public void testStrictColumnarModesNestedLeafArrayRoundTrip() throws Exception {
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
        DocumentMapper mapper = createMapperService(settings, mapping(b -> {
            b.startObject("comments");
            {
                b.field("type", "nested");
                b.startObject("properties");
                {
                    b.startObject("message").field("type", "keyword").endObject();
                    b.startObject("stars").field("type", "long").endObject();
                }
                b.endObject();
            }
            b.endObject();
        })).documentMapper();

        // A multi-valued leaf inside a nested object preserves array order: offsets are recorded per document, so each
        // nested child records its own offsets on its child document and the order is reconstructed when it is read back.
        String synthetic = syntheticSource(mapper, b -> {
            b.startArray("comments");
            {
                b.startObject().field("message", "first").array("stars", 50, 10, 30).endObject();
                b.startObject().field("message", "second").array("stars", 20, 40).endObject();
            }
            b.endArray();
        });
        assertThat(synthetic, equalTo("""
            {"comments":[{"message":"first","stars":[50,10,30]},{"message":"second","stars":[20,40]}]}"""));
    }

    private void assertNestedLeafArrayRoundTrip(String leafType, CheckedConsumer<XContentBuilder, IOException> doc, String expected)
        throws IOException {
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
        DocumentMapper mapper = createMapperService(settings, mapping(b -> {
            b.startObject("comments");
            {
                b.field("type", "nested");
                b.startObject("properties");
                b.startObject("v").field("type", leafType).endObject();
                b.endObject();
            }
            b.endObject();
        })).documentMapper();
        assertThat(syntheticSource(mapper, doc), equalTo(expected));
    }

    public void testStrictColumnarModesNestedKeywordArrayPreservesOrder() throws Exception {
        assertNestedLeafArrayRoundTrip("keyword", b -> {
            b.startArray("comments");
            b.startObject().array("v", "z", "a", "m").endObject();
            b.endArray();
        }, """
            {"comments":{"v":["z","a","m"]}}""");
    }

    public void testStrictColumnarModesNestedLeafArrayWithNulls() throws Exception {
        assertNestedLeafArrayRoundTrip("long", b -> {
            b.startArray("comments");
            b.startObject().startArray("v").value(50).nullValue().value(30).endArray().endObject();
            b.endArray();
        }, """
            {"comments":{"v":[50,null,30]}}""");
    }

    public void testStrictColumnarModesNestedLeafArrayWithDuplicates() throws Exception {
        assertNestedLeafArrayRoundTrip("long", b -> {
            b.startArray("comments");
            b.startObject().array("v", 50, 10, 50).endObject();
            b.endArray();
        }, """
            {"comments":{"v":[50,10,50]}}""");
    }

    public void testStrictColumnarModesNestedMultipleChildrenLeafArrays() throws Exception {
        assertNestedLeafArrayRoundTrip("long", b -> {
            b.startArray("comments");
            b.startObject().array("v", 50, 10).endObject();
            b.startObject().array("v", 20, 40, 30).endObject();
            b.endArray();
        }, """
            {"comments":[{"v":[50,10]},{"v":[20,40,30]}]}""");
    }

    public void testStrictColumnarModesRejectPassThroughInsideNested() {
        for (IndexMode indexMode : List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR)) {
            Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), indexMode.getName()).build();
            // A pass-through object's priority/alias is captured only at the columnar root, so inside a nested scope it is
            // rejected (like enabled:false / a dynamic override) rather than silently degrading to plain flattening.
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(settings, mapping(b -> {
                b.startObject("comments");
                {
                    b.field("type", "nested");
                    b.startObject("properties");
                    {
                        b.startObject("attributes");
                        {
                            b.field("type", "passthrough");
                            b.field("priority", 1);
                            b.startObject("properties");
                            b.startObject("host").field("type", "keyword").endObject();
                            b.endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            })));
            assertThat(e.getMessage(), containsString("the value of [type] is [passthrough]"));
        }
    }

    public void testStrictColumnarModesAllowHeterogeneousDynamic() throws Exception {
        for (IndexMode indexMode : List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR)) {
            Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), indexMode.getName()).build();
            // Root dynamic=true, attributes dynamic=false, resource dynamic=strict
            MapperService mapperService = createMapperService(settings, mapping(b -> {
                b.startObject("attributes");
                {
                    b.field("dynamic", "false");
                    b.startObject("properties");
                    b.startObject("host").field("type", "keyword").endObject();
                    b.endObject();
                }
                b.endObject();
                b.startObject("resource");
                {
                    b.field("dynamic", "strict");
                    b.startObject("properties");
                    b.startObject("service").field("type", "keyword").endObject();
                    b.endObject();
                }
                b.endObject();
            }));
            // Objects flattened — no ObjectMapper in the tree
            assertNull(mapperService.mappingLookup().objectMappers().get("attributes"));
            assertNull(mapperService.mappingLookup().objectMappers().get("resource"));
            // Leaf fields exist
            assertNotNull(mapperService.fieldType("attributes.host"));
            assertNotNull(mapperService.fieldType("resource.service"));
            // Prefix→dynamic map is populated
            RootObjectMapper root = mapperService.mappingLookup().getMapping().getRoot();
            assertEquals(ObjectMapper.Dynamic.FALSE, root.getPrefixProperties().get("attributes").dynamic());
            assertEquals(ObjectMapper.Dynamic.STRICT, root.getPrefixProperties().get("resource").dynamic());
        }
    }

    public void testStrictColumnarModesHeterogeneousDynamicNestedPrefixes() throws Exception {
        for (IndexMode indexMode : List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR)) {
            Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), indexMode.getName()).build();
            // foo dynamic=false, foo.bar dynamic=true (overrides parent)
            MapperService mapperService = createMapperService(settings, mapping(b -> {
                b.startObject("foo");
                {
                    b.field("dynamic", "false");
                    b.startObject("properties");
                    b.startObject("bar");
                    {
                        b.field("dynamic", "true");
                        b.startObject("properties");
                        b.startObject("x").field("type", "keyword").endObject();
                        b.endObject();
                    }
                    b.endObject();
                    b.endObject();
                }
                b.endObject();
            }));
            RootObjectMapper root = mapperService.mappingLookup().getMapping().getRoot();
            assertEquals(ObjectMapper.Dynamic.FALSE, root.getPrefixProperties().get("foo").dynamic());
            assertEquals(ObjectMapper.Dynamic.TRUE, root.getPrefixProperties().get("foo.bar").dynamic());
        }
    }

    public void testNonColumnarSubobjectsFalseStillRejectsMismatchedDynamic() {
        // Regression: plain subobjects:false (non-columnar) must still throw on dynamic mismatch
        MapperBuilderContext rootContext = MapperBuilderContext.root(false, false);
        ObjectMapper.Builder parentBuilder = new ObjectMapper.Builder("parent", Explicit.of(ObjectMapper.Subobjects.DISABLED)).add(
            new ObjectMapper.Builder("child").dynamic(ObjectMapper.Dynamic.FALSE)
        );
        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> parentBuilder.build(rootContext));
        assertThat(
            exception.getMessage(),
            containsString("the value of [dynamic] (FALSE) is not compatible with the value from its parent context")
        );
    }

    public void testStrictColumnarModesAllowEnabledFalse() throws Exception {
        for (IndexMode indexMode : List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR)) {
            Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), indexMode.getName()).build();
            MapperService mapperService = createMapperService(settings, mapping(b -> {
                b.startObject("attributes");
                {
                    b.field("enabled", false);
                    b.startObject("properties");
                    b.startObject("host").field("type", "keyword").endObject();
                    b.endObject();
                }
                b.endObject();
            }));
            // The disabled object is flattened away — no ObjectMapper in the tree
            assertNull(mapperService.mappingLookup().objectMappers().get("attributes"));
            // No children are flattened — the entire subtree is disabled.
            assertNull("children of an enabled:false object must not appear as indexed fields", mapperService.fieldType("attributes.host"));
            // The enabled:false prefix is captured in prefixProperties
            RootObjectMapper root = mapperService.mappingLookup().getMapping().getRoot();
            assertEquals(Boolean.FALSE, root.getPrefixProperties().get("attributes").enabled());
            // No dynamic facet for the disabled prefix
            assertNull(root.getPrefixProperties().get("attributes").dynamic());
        }
    }

    public void testNonColumnarSubobjectsFalseStillRejectsEnabledFalse() {
        // Regression: plain subobjects:false (non-columnar) must still throw for enabled:false
        MapperBuilderContext rootContext = MapperBuilderContext.root(false, false);
        ObjectMapper.Builder parentBuilder = new ObjectMapper.Builder("parent", Explicit.of(ObjectMapper.Subobjects.DISABLED)).add(
            new ObjectMapper.Builder("child").enabled(false)
        );
        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> parentBuilder.build(rootContext));
        assertThat(exception.getMessage(), containsString("the value of [enabled] is [false]"));
    }
}
