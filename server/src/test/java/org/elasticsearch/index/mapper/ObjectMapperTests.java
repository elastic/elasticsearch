/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Explicit;
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
            new CompressedXContent(BytesReference.bytes(topMapping(b -> b.field("dynamic", "strict"))))
        );
        Mapping merged = mapper.mapping().merge(mergeWith, reason, Long.MAX_VALUE);
        assertEquals(Dynamic.STRICT, merged.getRoot().dynamic());
    }

    public void testMergeEnabledForIndexTemplates() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {}));
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
        assertTrue(objectMapper.subobjects());

        // Setting 'enabled' to true is allowed, and updates the mapping.
        update = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("object")
                .field("type", "object")
                .field("enabled", true)
                .field("subobjects", false)
                .endObject()
                .endObject()
                .endObject()
        );
        mapper = mapperService.merge("type", new CompressedXContent(update), MergeReason.INDEX_TEMPLATE);

        objectMapper = mapper.mappers().objectMappers().get("object");
        assertNotNull(objectMapper);
        assertTrue(objectMapper.isEnabled());
        assertFalse(objectMapper.subobjects());
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
        Mapping mergeWith = mapperService.parseMapping("_doc", new CompressedXContent(BytesReference.bytes(fieldMapping(b -> {
            b.field("type", "object");
            b.field("subobjects", "false");
        }))));
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
        Mapping mergeWith = mapperService.parseMapping("_doc", new CompressedXContent(BytesReference.bytes(topMapping(b -> {
            b.field("subobjects", true);
        }))));
        MapperException exception = expectThrows(
            MapperException.class,
            () -> mapper.mapping().merge(mergeWith, MergeReason.MAPPING_UPDATE, Long.MAX_VALUE)
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
        assertThat(o.syntheticFieldLoader().docValuesLoader(null, null), nullValue());
        assertThat(mapper.mapping().getRoot().syntheticFieldLoader().docValuesLoader(null, null), nullValue());
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
        assertThat(o.syntheticFieldLoader().docValuesLoader(null, null), nullValue());
        assertThat(mapper.mapping().getRoot().syntheticFieldLoader().docValuesLoader(null, null), nullValue());
    }

    public void testNestedObjectWithMultiFieldsgetTotalFieldsCount() {
        ObjectMapper.Builder mapperBuilder = new ObjectMapper.Builder("parent_size_1", Explicit.IMPLICIT_TRUE).add(
            new ObjectMapper.Builder("child_size_2", Explicit.IMPLICIT_TRUE).add(
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
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("object");
            {
                b.field("type", "object");
                b.field("subobjects", false);
                b.field("enabled", false);
                b.field("dynamic", false);
                b.startObject("properties");
                propertiesBuilder.accept(b);
                b.endObject();
            }
            b.endObject();
        }));
        return (ObjectMapper) mapper.mapping().getRoot().getMapper("object");
    }

    public void testFlatten() {
        MapperBuilderContext rootContext = MapperBuilderContext.root(false, false);
        ObjectMapper objectMapper = new ObjectMapper.Builder("parent", Explicit.IMPLICIT_TRUE).add(
            new ObjectMapper.Builder("child", Explicit.IMPLICIT_TRUE).add(
                new KeywordFieldMapper.Builder("keyword2", IndexVersion.current())
            )
        ).add(new KeywordFieldMapper.Builder("keyword1", IndexVersion.current())).build(rootContext);
        List<String> fields = objectMapper.asFlattenedFieldMappers(rootContext).stream().map(FieldMapper::name).toList();
        assertThat(fields, containsInAnyOrder("parent.keyword1", "parent.child.keyword2"));
    }

    public void testFlattenDynamicIncompatible() {
        MapperBuilderContext rootContext = MapperBuilderContext.root(false, false);
        ObjectMapper objectMapper = new ObjectMapper.Builder("parent", Explicit.IMPLICIT_TRUE).add(
            new ObjectMapper.Builder("child", Explicit.IMPLICIT_TRUE).dynamic(Dynamic.FALSE)
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
        ObjectMapper objectMapper = new ObjectMapper.Builder("parent", Explicit.IMPLICIT_TRUE).enabled(false).build(rootContext);

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
        ObjectMapper objectMapper = new ObjectMapper.Builder("parent", Explicit.EXPLICIT_TRUE).build(rootContext);

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
}
