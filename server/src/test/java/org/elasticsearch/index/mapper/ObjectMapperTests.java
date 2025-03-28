/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.mapper.ObjectMapper.Dynamic;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.core.IsInstanceOf;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class ObjectMapperTests extends MapperServiceTestCase {

    public void testDifferentInnerObjectTokenFailure() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(mapping(b -> {}));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
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
        assertThat(e.getMessage(), containsString("can't merge a non object mapping [object.array.object] with an object mapping"));
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
        Mapping mergeWith = mapperService.parseMapping(
            "_doc",
            MergeReason.MAPPING_UPDATE,
            new CompressedXContent(BytesReference.bytes(topMapping(b -> b.field("dynamic", "strict"))))
        );
        Mapping merged = mapper.mapping().merge(mergeWith, reason, Long.MAX_VALUE);
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

        if (ObjectMapper.SUB_OBJECTS_AUTO_FEATURE_FLAG) {
            // Setting 'enabled' to true is allowed, and updates the mapping.
            update = Strings.toString(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("object")
                    .field("type", "object")
                    .field("enabled", true)
                    .field("subobjects", "auto")
                    .field(ObjectMapper.STORE_ARRAY_SOURCE_PARAM, true)
                    .endObject()
                    .endObject()
                    .endObject()
            );
            mapper = mapperService.merge("type", new CompressedXContent(update), MergeReason.INDEX_TEMPLATE);

            objectMapper = mapper.mappers().objectMappers().get("object");
            assertNotNull(objectMapper);
            assertTrue(objectMapper.isEnabled());
            assertEquals(ObjectMapper.Subobjects.AUTO, objectMapper.subobjects());
            assertEquals(Mapper.SourceKeepMode.ARRAYS, objectMapper.sourceKeepMode().orElse(Mapper.SourceKeepMode.NONE));
        }
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
        Mapping mergeWith = mapperService.parseMapping(
            "_doc",
            MergeReason.INDEX_TEMPLATE,
            new CompressedXContent(BytesReference.bytes(topMapping(b -> {
                b.startObject("properties").startObject("obj").field("type", "object").field("subobjects", false).startObject("properties");
                {
                    b.startObject("my.field").field("type", "long").endObject();
                }
                b.endObject().endObject().endObject();
            })))
        );

        // Fails on mapping update.
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> mapper.mapping().merge(mergeWith, MergeReason.MAPPING_UPDATE, Long.MAX_VALUE)
        );
        assertEquals("mapper [obj.my.field] cannot be changed from type [keyword] to [long]", exception.getMessage());

        // Passes on template merging.
        Mapping merged = mapper.mapping().merge(mergeWith, MergeReason.INDEX_TEMPLATE, Long.MAX_VALUE);
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

    public void testSubobjectsFalseWithInnerNested() {
        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> createMapperService(mapping(b -> {
            b.startObject("metrics.service");
            {
                b.field("subobjects", false);
                b.startObject("properties");
                {
                    b.startObject("time");
                    b.field("type", "nested");
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        })));
        assertEquals(
            "Failed to parse mapping: Tried to add nested object [time] to object [service] which does not support subobjects",
            exception.getMessage()
        );
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

    public void testSubobjectsFalseRootWithInnerNested() {
        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> createMapperService(mappingNoSubobjects(b -> {
            b.startObject("metrics.service");
            b.field("type", "nested");
            b.endObject();
        })));
        assertEquals(
            "Failed to parse mapping: Tried to add nested object [metrics.service] to object [_doc] which does not support subobjects",
            exception.getMessage()
        );
    }

    public void testSubobjectsCannotBeUpdated() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "object")));
        DocumentMapper mapper = mapperService.documentMapper();
        assertNull(mapper.mapping().getRoot().dynamic());
        Mapping mergeWith = mapperService.parseMapping(
            "_doc",
            MergeReason.MAPPING_UPDATE,
            new CompressedXContent(BytesReference.bytes(fieldMapping(b -> {
                b.field("type", "object");
                b.field("subobjects", "false");
            })))
        );
        MapperException exception = expectThrows(
            MapperException.class,
            () -> mapper.mapping().merge(mergeWith, MergeReason.MAPPING_UPDATE, Long.MAX_VALUE)
        );
        assertEquals("the [subobjects] parameter can't be updated for the object mapping [field]", exception.getMessage());
    }

    public void testSubobjectsCannotBeUpdatedOnRoot() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> b.field("subobjects", false)));
        DocumentMapper mapper = mapperService.documentMapper();
        assertNull(mapper.mapping().getRoot().dynamic());
        Mapping mergeWith = mapperService.parseMapping(
            "_doc",
            MergeReason.MAPPING_UPDATE,
            new CompressedXContent(BytesReference.bytes(topMapping(b -> {
                b.field("subobjects", true);
            })))
        );
        MapperException exception = expectThrows(
            MapperException.class,
            () -> mapper.mapping().merge(mergeWith, MergeReason.MAPPING_UPDATE, Long.MAX_VALUE)
        );
        assertEquals("the [subobjects] parameter can't be updated for the object mapping [_doc]", exception.getMessage());
    }

    public void testSubobjectsAuto() throws Exception {
        assumeTrue("only test when feature flag for subobjects auto is enabled", ObjectMapper.SUB_OBJECTS_AUTO_FEATURE_FLAG);
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("metrics.service");
            {
                b.field("subobjects", "auto");
                b.startObject("properties");
                {
                    b.startObject("time");
                    b.field("type", "long");
                    b.endObject();
                    b.startObject("time.max");
                    b.field("type", "long");
                    b.endObject();
                    b.startObject("attributes");
                    {
                        b.field("type", "object");
                        b.field("enabled", "false");
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));
        assertNotNull(mapperService.fieldType("metrics.service.time"));
        assertNotNull(mapperService.fieldType("metrics.service.time.max"));
        assertNotNull(mapperService.documentMapper().mappers().objectMappers().get("metrics.service.attributes"));
    }

    public void testSubobjectsAutoWithInnerObject() throws IOException {
        assumeTrue("only test when feature flag for subobjects auto is enabled", ObjectMapper.SUB_OBJECTS_AUTO_FEATURE_FLAG);
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("metrics.service");
            {
                b.field("subobjects", "auto");
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
                    b.startObject("foo");
                    b.field("type", "keyword");
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));
        assertNull(mapperService.fieldType("metrics.service.time"));
        assertNotNull(mapperService.fieldType("metrics.service.time.max"));
        assertNotNull(mapperService.fieldType("metrics.service.foo"));
        assertNotNull(mapperService.documentMapper().mappers().objectMappers().get("metrics.service.time"));
        assertNotNull(mapperService.documentMapper().mappers().getMapper("metrics.service.foo"));
    }

    public void testSubobjectsAutoWithInnerNested() throws IOException {
        assumeTrue("only test when feature flag for subobjects auto is enabled", ObjectMapper.SUB_OBJECTS_AUTO_FEATURE_FLAG);
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("metrics.service");
            {
                b.field("subobjects", "auto");
                b.startObject("properties");
                {
                    b.startObject("time");
                    b.field("type", "nested");
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));
        assertThat(
            mapperService.documentMapper().mappers().objectMappers().get("metrics.service.time"),
            IsInstanceOf.instanceOf(NestedObjectMapper.class)
        );
    }

    public void testSubobjectsAutoRoot() throws Exception {
        assumeTrue("only test when feature flag for subobjects auto is enabled", ObjectMapper.SUB_OBJECTS_AUTO_FEATURE_FLAG);
        MapperService mapperService = createMapperService(mappingWithSubobjects(b -> {
            b.startObject("metrics.service.time");
            b.field("type", "long");
            b.endObject();
            b.startObject("metrics.service.time.max");
            b.field("type", "long");
            b.endObject();
            b.startObject("metrics.attributes");
            {
                b.field("type", "object");
                b.field("enabled", "false");
            }
            b.endObject();
        }, "auto"));
        assertNotNull(mapperService.fieldType("metrics.service.time"));
        assertNotNull(mapperService.fieldType("metrics.service.time.max"));
        assertNotNull(mapperService.documentMapper().mappers().objectMappers().get("metrics.attributes"));
    }

    public void testSubobjectsAutoRootWithInnerObject() throws IOException {
        assumeTrue("only test when feature flag for subobjects auto is enabled", ObjectMapper.SUB_OBJECTS_AUTO_FEATURE_FLAG);
        MapperService mapperService = createMapperService(mappingWithSubobjects(b -> {
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
        }, "auto"));
        assertNull(mapperService.fieldType("metrics.service.time"));
        assertNotNull(mapperService.fieldType("metrics.service.time.max"));
        assertNotNull(mapperService.documentMapper().mappers().objectMappers().get("metrics.service.time"));
        assertNotNull(mapperService.documentMapper().mappers().getMapper("metrics.service.time.max"));
    }

    public void testSubobjectsAutoRootWithInnerNested() throws IOException {
        assumeTrue("only test when feature flag for subobjects auto is enabled", ObjectMapper.SUB_OBJECTS_AUTO_FEATURE_FLAG);
        MapperService mapperService = createMapperService(mappingWithSubobjects(b -> {
            b.startObject("metrics.service");
            b.field("type", "nested");
            b.endObject();
        }, "auto"));
        assertThat(
            mapperService.documentMapper().mappers().objectMappers().get("metrics.service"),
            IsInstanceOf.instanceOf(NestedObjectMapper.class)
        );
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

    public void testStoreArraySourceinSyntheticSourceMode() throws IOException {
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
        ObjectMapper.Builder mapperBuilder = new ObjectMapper.Builder("parent_size_1", Optional.empty()).add(
            new ObjectMapper.Builder("child_size_2", Optional.empty()).add(
                new TextFieldMapper.Builder("grand_child_size_3", createDefaultIndexAnalyzers(), false).addMultiField(
                    new KeywordFieldMapper.Builder("multi_field_size_4", IndexVersion.current())
                )
                    .addMultiField(
                        new TextFieldMapper.Builder("grand_child_size_5", createDefaultIndexAnalyzers(), false).addMultiField(
                            new KeywordFieldMapper.Builder("multi_field_of_multi_field_size_6", IndexVersion.current())
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
        ObjectMapper objectMapper = new ObjectMapper.Builder("parent", Optional.empty()).add(
            new ObjectMapper.Builder("child", Optional.empty()).add(new KeywordFieldMapper.Builder("keyword2", IndexVersion.current()))
        ).add(new KeywordFieldMapper.Builder("keyword1", IndexVersion.current())).build(rootContext);
        List<String> fields = objectMapper.asFlattenedFieldMappers(rootContext).stream().map(FieldMapper::fullPath).toList();
        assertThat(fields, containsInAnyOrder("parent.keyword1", "parent.child.keyword2"));
    }

    public void testFlattenSubobjectsAuto() {
        MapperBuilderContext rootContext = MapperBuilderContext.root(false, false);
        ObjectMapper objectMapper = new ObjectMapper.Builder("parent", Optional.of(ObjectMapper.Subobjects.AUTO)).add(
            new ObjectMapper.Builder("child", Optional.empty()).add(new KeywordFieldMapper.Builder("keyword2", IndexVersion.current()))
        ).add(new KeywordFieldMapper.Builder("keyword1", IndexVersion.current())).build(rootContext);
        List<String> fields = objectMapper.asFlattenedFieldMappers(rootContext).stream().map(FieldMapper::fullPath).toList();
        assertThat(fields, containsInAnyOrder("parent.keyword1", "parent.child.keyword2"));
    }

    public void testFlattenSubobjectsFalse() {
        MapperBuilderContext rootContext = MapperBuilderContext.root(false, false);
        ObjectMapper objectMapper = new ObjectMapper.Builder("parent", Optional.of(ObjectMapper.Subobjects.DISABLED)).add(
            new ObjectMapper.Builder("child", Optional.empty()).add(new KeywordFieldMapper.Builder("keyword2", IndexVersion.current()))
        ).add(new KeywordFieldMapper.Builder("keyword1", IndexVersion.current())).build(rootContext);
        List<String> fields = objectMapper.asFlattenedFieldMappers(rootContext).stream().map(FieldMapper::fullPath).toList();
        assertThat(fields, containsInAnyOrder("parent.keyword1", "parent.child.keyword2"));
    }

    public void testFlattenDynamicIncompatible() {
        MapperBuilderContext rootContext = MapperBuilderContext.root(false, false);
        ObjectMapper objectMapper = new ObjectMapper.Builder("parent", Optional.empty()).add(
            new ObjectMapper.Builder("child", Optional.empty()).dynamic(Dynamic.FALSE)
        ).build(rootContext);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> objectMapper.asFlattenedFieldMappers(rootContext)
        );
        assertEquals(
            "Object mapper [parent.child] was found in a context where subobjects is set to false. "
                + "Auto-flattening [parent.child] failed because the value of [dynamic] (FALSE) is not compatible with "
                + "the value from its parent context (TRUE)",
            exception.getMessage()
        );
    }

    public void testFlattenEnabledFalse() {
        MapperBuilderContext rootContext = MapperBuilderContext.root(false, false);
        ObjectMapper objectMapper = new ObjectMapper.Builder("parent", Optional.empty()).enabled(false).build(rootContext);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> objectMapper.asFlattenedFieldMappers(rootContext)
        );
        assertEquals(
            "Object mapper [parent] was found in a context where subobjects is set to false. "
                + "Auto-flattening [parent] failed because the value of [enabled] is [false]",
            exception.getMessage()
        );
    }

    public void testFlattenExplicitSubobjectsTrue() {
        MapperBuilderContext rootContext = MapperBuilderContext.root(false, false);
        ObjectMapper objectMapper = new ObjectMapper.Builder("parent", Optional.of(ObjectMapper.Subobjects.ENABLED)).build(rootContext);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> objectMapper.asFlattenedFieldMappers(rootContext)
        );
        assertEquals(
            "Object mapper [parent] was found in a context where subobjects is set to false. "
                + "Auto-flattening [parent] failed because the value of [subobjects] is [true]",
            exception.getMessage()
        );
    }

    public void testFindParentMapper() {
        MapperBuilderContext rootContext = MapperBuilderContext.root(false, false);

        var rootBuilder = new RootObjectMapper.Builder("_doc", Optional.empty());
        rootBuilder.add(new KeywordFieldMapper.Builder("keyword", IndexVersion.current()));

        var child = new ObjectMapper.Builder("child", Optional.empty());
        child.add(new KeywordFieldMapper.Builder("keyword2", IndexVersion.current()));
        child.add(new KeywordFieldMapper.Builder("keyword.with.dot", IndexVersion.current()));
        var secondLevelChild = new ObjectMapper.Builder("child2", Optional.empty());
        secondLevelChild.add(new KeywordFieldMapper.Builder("keyword22", IndexVersion.current()));
        child.add(secondLevelChild);
        rootBuilder.add(child);

        var childWithDot = new ObjectMapper.Builder("childwith.dot", Optional.empty());
        childWithDot.add(new KeywordFieldMapper.Builder("keyword3", IndexVersion.current()));
        childWithDot.add(new KeywordFieldMapper.Builder("keyword4.with.dot", IndexVersion.current()));
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
}
