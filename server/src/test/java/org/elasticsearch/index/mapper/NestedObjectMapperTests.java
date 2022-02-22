/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.mapper.ObjectMapper.Dynamic;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class NestedObjectMapperTests extends MapperServiceTestCase {

    public void testEmptyNested() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> b.startObject("nested1").field("type", "nested").endObject()));

        ParsedDocument doc = docMapper.parse(source(b -> b.field("field", "value").nullField("nested1")));

        assertThat(doc.docs().size(), equalTo(1));

        doc = docMapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(
                    XContentFactory.jsonBuilder().startObject().field("field", "value").startArray("nested").endArray().endObject()
                ),
                XContentType.JSON
            )
        );

        assertThat(doc.docs().size(), equalTo(1));
    }

    public void testSingleNested() throws Exception {

        DocumentMapper docMapper = createDocumentMapper(mapping(b -> b.startObject("nested1").field("type", "nested").endObject()));

        assertNotEquals(NestedLookup.EMPTY, docMapper.mappers().nestedLookup());
        ObjectMapper mapper = docMapper.mappers().objectMappers().get("nested1");
        assertThat(mapper, instanceOf(NestedObjectMapper.class));
        NestedObjectMapper nested1Mapper = (NestedObjectMapper) mapper;

        ParsedDocument doc = docMapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "value")
                        .startObject("nested1")
                        .field("field1", "1")
                        .field("field2", "2")
                        .endObject()
                        .endObject()
                ),
                XContentType.JSON
            )
        );

        assertThat(doc.docs().size(), equalTo(2));
        assertThat(doc.docs().get(0).get(NestedPathFieldMapper.NAME), equalTo(nested1Mapper.nestedTypePath()));
        assertThat(doc.docs().get(0).get("nested1.field1"), equalTo("1"));
        assertThat(doc.docs().get(0).get("nested1.field2"), equalTo("2"));

        assertThat(doc.docs().get(1).get("field"), equalTo("value"));

        doc = docMapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "value")
                        .startArray("nested1")
                        .startObject()
                        .field("field1", "1")
                        .field("field2", "2")
                        .endObject()
                        .startObject()
                        .field("field1", "3")
                        .field("field2", "4")
                        .endObject()
                        .endArray()
                        .endObject()
                ),
                XContentType.JSON
            )
        );

        assertThat(doc.docs().size(), equalTo(3));
        assertThat(doc.docs().get(0).get(NestedPathFieldMapper.NAME), equalTo(nested1Mapper.nestedTypePath()));
        assertThat(doc.docs().get(0).get("nested1.field1"), equalTo("1"));
        assertThat(doc.docs().get(0).get("nested1.field2"), equalTo("2"));
        assertThat(doc.docs().get(1).get(NestedPathFieldMapper.NAME), equalTo(nested1Mapper.nestedTypePath()));
        assertThat(doc.docs().get(1).get("nested1.field1"), equalTo("3"));
        assertThat(doc.docs().get(1).get("nested1.field2"), equalTo("4"));

        assertThat(doc.docs().get(2).get("field"), equalTo("value"));
    }

    public void testMultiNested() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {
            b.startObject("nested1");
            {
                b.field("type", "nested");
                b.startObject("properties");
                {
                    b.startObject("nested2").field("type", "nested").endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        assertNotEquals(NestedLookup.EMPTY, docMapper.mappers().nestedLookup());
        ObjectMapper mapper1 = docMapper.mappers().objectMappers().get("nested1");
        assertThat(mapper1, instanceOf(NestedObjectMapper.class));
        NestedObjectMapper nested1Mapper = (NestedObjectMapper) mapper1;
        assertThat(nested1Mapper.isIncludeInParent(), equalTo(false));
        assertThat(nested1Mapper.isIncludeInRoot(), equalTo(false));
        ObjectMapper mapper2 = docMapper.mappers().objectMappers().get("nested1.nested2");
        assertThat(mapper2, instanceOf(NestedObjectMapper.class));
        NestedObjectMapper nested2Mapper = (NestedObjectMapper) mapper2;
        assertThat(nested2Mapper.isIncludeInParent(), equalTo(false));
        assertThat(nested2Mapper.isIncludeInRoot(), equalTo(false));

        ParsedDocument doc = docMapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "value")
                        .startArray("nested1")
                        .startObject()
                        .field("field1", "1")
                        .startArray("nested2")
                        .startObject()
                        .field("field2", "2")
                        .endObject()
                        .startObject()
                        .field("field2", "3")
                        .endObject()
                        .endArray()
                        .endObject()
                        .startObject()
                        .field("field1", "4")
                        .startArray("nested2")
                        .startObject()
                        .field("field2", "5")
                        .endObject()
                        .startObject()
                        .field("field2", "6")
                        .endObject()
                        .endArray()
                        .endObject()
                        .endArray()
                        .endObject()
                ),
                XContentType.JSON
            )
        );

        assertThat(doc.docs().size(), equalTo(7));
        assertThat(doc.docs().get(0).get("nested1.nested2.field2"), equalTo("2"));
        assertThat(doc.docs().get(0).get("nested1.field1"), nullValue());
        assertThat(doc.docs().get(0).get("field"), nullValue());
        assertThat(doc.docs().get(1).get("nested1.nested2.field2"), equalTo("3"));
        assertThat(doc.docs().get(1).get("nested1.field1"), nullValue());
        assertThat(doc.docs().get(1).get("field"), nullValue());
        assertThat(doc.docs().get(2).get("nested1.field1"), equalTo("1"));
        assertThat(doc.docs().get(2).get("nested1.nested2.field2"), nullValue());
        assertThat(doc.docs().get(2).get("field"), nullValue());
        assertThat(doc.docs().get(3).get("nested1.nested2.field2"), equalTo("5"));
        assertThat(doc.docs().get(3).get("field"), nullValue());
        assertThat(doc.docs().get(4).get("nested1.nested2.field2"), equalTo("6"));
        assertThat(doc.docs().get(4).get("field"), nullValue());
        assertThat(doc.docs().get(5).get("nested1.field1"), equalTo("4"));
        assertThat(doc.docs().get(5).get("nested1.nested2.field2"), nullValue());
        assertThat(doc.docs().get(5).get("field"), nullValue());
        assertThat(doc.docs().get(6).get("field"), equalTo("value"));
        assertThat(doc.docs().get(6).get("nested1.field1"), nullValue());
        assertThat(doc.docs().get(6).get("nested1.nested2.field2"), nullValue());
    }

    public void testMultiObjectAndNested1() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {
            b.startObject("nested1");
            {
                b.field("type", "nested");
                b.startObject("properties");
                {
                    b.startObject("nested2");
                    {
                        b.field("type", "nested");
                        b.field("include_in_parent", true);
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        assertNotEquals(NestedLookup.EMPTY, docMapper.mappers().nestedLookup());
        ObjectMapper mapper1 = docMapper.mappers().objectMappers().get("nested1");
        assertThat(mapper1, instanceOf(NestedObjectMapper.class));
        NestedObjectMapper nested1Mapper = (NestedObjectMapper) mapper1;
        assertThat(nested1Mapper.isIncludeInParent(), equalTo(false));
        assertThat(nested1Mapper.isIncludeInRoot(), equalTo(false));
        ObjectMapper mapper2 = docMapper.mappers().objectMappers().get("nested1.nested2");
        assertThat(mapper2, instanceOf(NestedObjectMapper.class));
        NestedObjectMapper nested2Mapper = (NestedObjectMapper) mapper2;
        assertThat(nested2Mapper.isIncludeInParent(), equalTo(true));
        assertThat(nested2Mapper.isIncludeInRoot(), equalTo(false));

        ParsedDocument doc = docMapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "value")
                        .startArray("nested1")
                        .startObject()
                        .field("field1", "1")
                        .startArray("nested2")
                        .startObject()
                        .field("field2", "2")
                        .endObject()
                        .startObject()
                        .field("field2", "3")
                        .endObject()
                        .endArray()
                        .endObject()
                        .startObject()
                        .field("field1", "4")
                        .startArray("nested2")
                        .startObject()
                        .field("field2", "5")
                        .endObject()
                        .startObject()
                        .field("field2", "6")
                        .endObject()
                        .endArray()
                        .endObject()
                        .endArray()
                        .endObject()
                ),
                XContentType.JSON
            )
        );

        assertThat(doc.docs().size(), equalTo(7));
        assertThat(doc.docs().get(0).get("nested1.nested2.field2"), equalTo("2"));
        assertThat(doc.docs().get(0).get("nested1.field1"), nullValue());
        assertThat(doc.docs().get(0).get("field"), nullValue());
        assertThat(doc.docs().get(1).get("nested1.nested2.field2"), equalTo("3"));
        assertThat(doc.docs().get(1).get("nested1.field1"), nullValue());
        assertThat(doc.docs().get(1).get("field"), nullValue());
        assertThat(doc.docs().get(2).get("nested1.field1"), equalTo("1"));
        assertThat(doc.docs().get(2).get("nested1.nested2.field2"), equalTo("2"));
        assertThat(doc.docs().get(2).get("field"), nullValue());
        assertThat(doc.docs().get(3).get("nested1.nested2.field2"), equalTo("5"));
        assertThat(doc.docs().get(3).get("field"), nullValue());
        assertThat(doc.docs().get(4).get("nested1.nested2.field2"), equalTo("6"));
        assertThat(doc.docs().get(4).get("field"), nullValue());
        assertThat(doc.docs().get(5).get("nested1.field1"), equalTo("4"));
        assertThat(doc.docs().get(5).get("nested1.nested2.field2"), equalTo("5"));
        assertThat(doc.docs().get(5).get("field"), nullValue());
        assertThat(doc.docs().get(6).get("field"), equalTo("value"));
        assertThat(doc.docs().get(6).get("nested1.field1"), nullValue());
        assertThat(doc.docs().get(6).get("nested1.nested2.field2"), nullValue());
    }

    public void testMultiObjectAndNested2() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {
            b.startObject("nested1");
            {
                b.field("type", "nested");
                b.field("include_in_parent", true);
                b.startObject("properties");
                {
                    b.startObject("nested2");
                    {
                        b.field("type", "nested");
                        b.field("include_in_parent", true);
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        assertNotEquals(NestedLookup.EMPTY, docMapper.mappers().nestedLookup());
        ObjectMapper mapper1 = docMapper.mappers().objectMappers().get("nested1");
        assertThat(mapper1, instanceOf(NestedObjectMapper.class));
        NestedObjectMapper nested1Mapper = (NestedObjectMapper) mapper1;
        assertThat(nested1Mapper.isIncludeInParent(), equalTo(true));
        assertThat(nested1Mapper.isIncludeInRoot(), equalTo(false));
        ObjectMapper mapper2 = docMapper.mappers().objectMappers().get("nested1.nested2");
        assertThat(mapper2, instanceOf(NestedObjectMapper.class));
        NestedObjectMapper nested2Mapper = (NestedObjectMapper) mapper2;
        assertThat(nested2Mapper.isIncludeInParent(), equalTo(true));
        assertThat(nested2Mapper.isIncludeInRoot(), equalTo(false));

        ParsedDocument doc = docMapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "value")
                        .startArray("nested1")
                        .startObject()
                        .field("field1", "1")
                        .startArray("nested2")
                        .startObject()
                        .field("field2", "2")
                        .endObject()
                        .startObject()
                        .field("field2", "3")
                        .endObject()
                        .endArray()
                        .endObject()
                        .startObject()
                        .field("field1", "4")
                        .startArray("nested2")
                        .startObject()
                        .field("field2", "5")
                        .endObject()
                        .startObject()
                        .field("field2", "6")
                        .endObject()
                        .endArray()
                        .endObject()
                        .endArray()
                        .endObject()
                ),
                XContentType.JSON
            )
        );

        assertThat(doc.docs().size(), equalTo(7));
        assertThat(doc.docs().get(0).get("nested1.nested2.field2"), equalTo("2"));
        assertThat(doc.docs().get(0).get("nested1.field1"), nullValue());
        assertThat(doc.docs().get(0).get("field"), nullValue());
        assertThat(doc.docs().get(1).get("nested1.nested2.field2"), equalTo("3"));
        assertThat(doc.docs().get(1).get("nested1.field1"), nullValue());
        assertThat(doc.docs().get(1).get("field"), nullValue());
        assertThat(doc.docs().get(2).get("nested1.field1"), equalTo("1"));
        assertThat(doc.docs().get(2).get("nested1.nested2.field2"), equalTo("2"));
        assertThat(doc.docs().get(2).get("field"), nullValue());
        assertThat(doc.docs().get(3).get("nested1.nested2.field2"), equalTo("5"));
        assertThat(doc.docs().get(3).get("field"), nullValue());
        assertThat(doc.docs().get(4).get("nested1.nested2.field2"), equalTo("6"));
        assertThat(doc.docs().get(4).get("field"), nullValue());
        assertThat(doc.docs().get(5).get("nested1.field1"), equalTo("4"));
        assertThat(doc.docs().get(5).get("nested1.nested2.field2"), equalTo("5"));
        assertThat(doc.docs().get(5).get("field"), nullValue());
        assertThat(doc.docs().get(6).get("field"), equalTo("value"));
        assertThat(doc.docs().get(6).getFields("nested1.field1").length, equalTo(2));
        assertThat(doc.docs().get(6).getFields("nested1.nested2.field2").length, equalTo(4));
    }

    public void testMultiRootAndNested1() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {
            b.startObject("nested1");
            {
                b.field("type", "nested");
                b.startObject("properties");
                {
                    b.startObject("nested2");
                    {
                        b.field("type", "nested");
                        b.field("include_in_root", true);
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        assertEquals("nested1", docMapper.mappers().nestedLookup().getNestedParent("nested1.nested2"));
        assertNull(docMapper.mappers().nestedLookup().getNestedParent("nonexistent"));
        assertNull(docMapper.mappers().nestedLookup().getNestedParent("nested1"));

        assertNotEquals(NestedLookup.EMPTY, docMapper.mappers().nestedLookup());
        ObjectMapper mapper1 = docMapper.mappers().objectMappers().get("nested1");
        assertThat(mapper1, instanceOf(NestedObjectMapper.class));
        NestedObjectMapper nested1Mapper = (NestedObjectMapper) mapper1;
        assertThat(nested1Mapper.isIncludeInParent(), equalTo(false));
        assertThat(nested1Mapper.isIncludeInRoot(), equalTo(false));
        ObjectMapper mapper2 = docMapper.mappers().objectMappers().get("nested1.nested2");
        assertThat(mapper2, instanceOf(NestedObjectMapper.class));
        NestedObjectMapper nested2Mapper = (NestedObjectMapper) mapper2;
        assertThat(nested2Mapper.isIncludeInParent(), equalTo(false));
        assertThat(nested2Mapper.isIncludeInRoot(), equalTo(true));

        ParsedDocument doc = docMapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "value")
                        .startArray("nested1")
                        .startObject()
                        .field("field1", "1")
                        .startArray("nested2")
                        .startObject()
                        .field("field2", "2")
                        .endObject()
                        .startObject()
                        .field("field2", "3")
                        .endObject()
                        .endArray()
                        .endObject()
                        .startObject()
                        .field("field1", "4")
                        .startArray("nested2")
                        .startObject()
                        .field("field2", "5")
                        .endObject()
                        .startObject()
                        .field("field2", "6")
                        .endObject()
                        .endArray()
                        .endObject()
                        .endArray()
                        .endObject()
                ),
                XContentType.JSON
            )
        );

        assertThat(doc.docs().size(), equalTo(7));
        assertThat(doc.docs().get(0).get("nested1.nested2.field2"), equalTo("2"));
        assertThat(doc.docs().get(0).get("nested1.field1"), nullValue());
        assertThat(doc.docs().get(0).get("field"), nullValue());
        assertThat(doc.docs().get(1).get("nested1.nested2.field2"), equalTo("3"));
        assertThat(doc.docs().get(1).get("nested1.field1"), nullValue());
        assertThat(doc.docs().get(1).get("field"), nullValue());
        assertThat(doc.docs().get(2).get("nested1.field1"), equalTo("1"));
        assertThat(doc.docs().get(2).get("nested1.nested2.field2"), nullValue());
        assertThat(doc.docs().get(2).get("field"), nullValue());
        assertThat(doc.docs().get(3).get("nested1.nested2.field2"), equalTo("5"));
        assertThat(doc.docs().get(3).get("field"), nullValue());
        assertThat(doc.docs().get(4).get("nested1.nested2.field2"), equalTo("6"));
        assertThat(doc.docs().get(4).get("field"), nullValue());
        assertThat(doc.docs().get(5).get("nested1.field1"), equalTo("4"));
        assertThat(doc.docs().get(5).get("nested1.nested2.field2"), nullValue());
        assertThat(doc.docs().get(5).get("field"), nullValue());
        assertThat(doc.docs().get(6).get("field"), equalTo("value"));
        assertThat(doc.docs().get(6).get("nested1.field1"), nullValue());
        assertThat(doc.docs().get(6).getFields("nested1.nested2.field2").length, equalTo(4));
    }

    /**
     * Checks that multiple levels of nested includes where a node is both directly and transitively
     * included in root by {@code include_in_root} and a chain of {@code include_in_parent} does not
     * lead to duplicate fields on the root document.
     */
    public void testMultipleLevelsIncludeRoot1() throws Exception {
        MapperService mapperService = createMapperService(mapping(b -> {}));

        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject(MapperService.SINGLE_MAPPING_NAME)
                .startObject("properties")
                .startObject("nested1")
                .field("type", "nested")
                .field("include_in_root", true)
                .field("include_in_parent", true)
                .startObject("properties")
                .startObject("nested2")
                .field("type", "nested")
                .field("include_in_root", true)
                .field("include_in_parent", true)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );
        MergeReason mergeReason = randomFrom(MergeReason.MAPPING_UPDATE, MergeReason.INDEX_TEMPLATE);

        mapperService.merge(MapperService.SINGLE_MAPPING_NAME, new CompressedXContent(mapping), mergeReason);
        DocumentMapper docMapper = mapperService.documentMapper();

        ParsedDocument doc = docMapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startArray("nested1")
                        .startObject()
                        .startArray("nested2")
                        .startObject()
                        .field("foo", "bar")
                        .endObject()
                        .endArray()
                        .endObject()
                        .endArray()
                        .endObject()
                ),
                XContentType.JSON
            )
        );

        final Collection<IndexableField> fields = doc.rootDoc().getFields();
        assertThat(fields.size(), equalTo(new HashSet<>(fields).size()));
    }

    public void testRecursiveIncludeInParent() throws IOException {

        // if we have a nested hierarchy, and all nested mappers have 'include_in_parent'
        // set to 'true', then values from the grandchild nodes should be copied all the
        // way up the hierarchy and into the root document, even if 'include_in_root' has
        // explicitly been set to 'false'.

        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("nested1");
            b.field("type", "nested");
            b.field("include_in_parent", true);
            b.field("include_in_root", false);
            b.startObject("properties");
            b.startObject("nested1_id").field("type", "keyword").endObject();
            b.startObject("nested2");
            b.field("type", "nested");
            b.field("include_in_parent", true);
            b.field("include_in_root", false);
            b.startObject("properties");
            b.startObject("nested2_id").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
            b.endObject();
            b.endObject();
        }));

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> {
            b.startObject("nested1");
            b.field("nested1_id", "1");
            b.startObject("nested2");
            b.field("nested2_id", "2");
            b.endObject();
            b.endObject();
        }));

        assertNotNull(doc.rootDoc().getField("nested1.nested2.nested2_id"));
    }

    /**
     * Same as {@link NestedObjectMapperTests#testMultipleLevelsIncludeRoot1()} but tests for the
     * case where the transitive {@code include_in_parent} and redundant {@code include_in_root}
     * happen on a chain of nodes that starts from a parent node that is not directly connected to
     * root by a chain of {@code include_in_parent}, i.e. that has {@code include_in_parent} set to
     * {@code false} and {@code include_in_root} set to {@code true}.
     */
    public void testMultipleLevelsIncludeRoot2() throws Exception {
        MapperService mapperService = createMapperService(mapping(b -> {}));

        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject(MapperService.SINGLE_MAPPING_NAME)
                .startObject("properties")
                .startObject("nested1")
                .field("type", "nested")
                .field("include_in_root", true)
                .field("include_in_parent", true)
                .startObject("properties")
                .startObject("nested2")
                .field("type", "nested")
                .field("include_in_root", true)
                .field("include_in_parent", false)
                .startObject("properties")
                .startObject("nested3")
                .field("type", "nested")
                .field("include_in_root", true)
                .field("include_in_parent", true)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );
        MergeReason mergeReason = randomFrom(MergeReason.MAPPING_UPDATE, MergeReason.INDEX_TEMPLATE);

        mapperService.merge(MapperService.SINGLE_MAPPING_NAME, new CompressedXContent(mapping), mergeReason);
        DocumentMapper docMapper = mapperService.documentMapper();

        ParsedDocument doc = docMapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startArray("nested1")
                        .startObject()
                        .startArray("nested2")
                        .startObject()
                        .startArray("nested3")
                        .startObject()
                        .field("foo", "bar")
                        .endObject()
                        .endArray()
                        .endObject()
                        .endArray()
                        .endObject()
                        .endArray()
                        .endObject()
                ),
                XContentType.JSON
            )
        );

        final Collection<IndexableField> fields = doc.rootDoc().getFields();
        assertThat(fields.size(), equalTo(new HashSet<>(fields).size()));
    }

    /**
     * Same as {@link NestedObjectMapperTests#testMultipleLevelsIncludeRoot1()} but tests that
     * the redundant includes are removed even if each individual mapping doesn't contain the
     * redundancy, only the merged mapping does.
     */
    public void testMultipleLevelsIncludeRootWithMerge() throws Exception {
        MapperService mapperService = createMapperService(mapping(b -> {}));

        String firstMapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject(MapperService.SINGLE_MAPPING_NAME)
                .startObject("properties")
                .startObject("nested1")
                .field("type", "nested")
                .field("include_in_root", true)
                .startObject("properties")
                .startObject("nested2")
                .field("type", "nested")
                .field("include_in_root", true)
                .field("include_in_parent", true)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );
        mapperService.merge(MapperService.SINGLE_MAPPING_NAME, new CompressedXContent(firstMapping), MergeReason.INDEX_TEMPLATE);

        String secondMapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject(MapperService.SINGLE_MAPPING_NAME)
                .startObject("properties")
                .startObject("nested1")
                .field("type", "nested")
                .field("include_in_root", true)
                .field("include_in_parent", true)
                .startObject("properties")
                .startObject("nested2")
                .field("type", "nested")
                .field("include_in_root", true)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        mapperService.merge(MapperService.SINGLE_MAPPING_NAME, new CompressedXContent(secondMapping), MergeReason.INDEX_TEMPLATE);
        DocumentMapper docMapper = mapperService.documentMapper();

        ParsedDocument doc = docMapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startArray("nested1")
                        .startObject()
                        .startArray("nested2")
                        .startObject()
                        .field("foo", "bar")
                        .endObject()
                        .endArray()
                        .endObject()
                        .endArray()
                        .endObject()
                ),
                XContentType.JSON
            )
        );

        final Collection<IndexableField> fields = doc.rootDoc().getFields();
        assertThat(fields.size(), equalTo(new HashSet<>(fields).size()));
    }

    public void testNestedArrayStrict() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {
            b.startObject("nested1");
            {
                b.field("type", "nested");
                b.field("dynamic", "strict");
                b.startObject("properties");
                {
                    b.startObject("field1").field("type", "text").endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        assertNotEquals(NestedLookup.EMPTY, docMapper.mappers().nestedLookup());
        ObjectMapper nested1Mapper = docMapper.mappers().objectMappers().get("nested1");
        assertThat(nested1Mapper, instanceOf(NestedObjectMapper.class));
        assertThat(nested1Mapper.dynamic(), equalTo(Dynamic.STRICT));

        ParsedDocument doc = docMapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "value")
                        .startArray("nested1")
                        .startObject()
                        .field("field1", "1")
                        .endObject()
                        .startObject()
                        .field("field1", "4")
                        .endObject()
                        .endArray()
                        .endObject()
                ),
                XContentType.JSON
            )
        );

        assertThat(doc.docs().size(), equalTo(3));
        assertThat(doc.docs().get(0).get("nested1.field1"), equalTo("1"));
        assertThat(doc.docs().get(0).get("field"), nullValue());
        assertThat(doc.docs().get(1).get("nested1.field1"), equalTo("4"));
        assertThat(doc.docs().get(1).get("field"), nullValue());
        assertThat(doc.docs().get(2).get("field"), equalTo("value"));
    }

    public void testLimitOfNestedFieldsPerIndex() throws Exception {
        Function<String, String> mapping = type -> {
            try {
                return Strings.toString(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject(type)
                        .startObject("properties")
                        .startObject("nested1")
                        .field("type", "nested")
                        .startObject("properties")
                        .startObject("nested2")
                        .field("type", "nested")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                );
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };

        // default limit allows at least two nested fields
        createMapperService(mapping.apply("_doc"));

        // explicitly setting limit to 0 prevents nested fields
        Exception e = expectThrows(IllegalArgumentException.class, () -> {
            Settings settings = Settings.builder().put(MapperService.INDEX_MAPPING_NESTED_FIELDS_LIMIT_SETTING.getKey(), 0).build();
            createMapperService(settings, mapping.apply("_doc"));
        });
        assertThat(e.getMessage(), containsString("Limit of nested fields [0] has been exceeded"));

        // setting limit to 1 with 2 nested fields fails
        e = expectThrows(IllegalArgumentException.class, () -> {
            Settings settings = Settings.builder().put(MapperService.INDEX_MAPPING_NESTED_FIELDS_LIMIT_SETTING.getKey(), 1).build();
            createMapperService(settings, mapping.apply("_doc"));
        });
        assertThat(e.getMessage(), containsString("Limit of nested fields [1] has been exceeded"));

        // do not check nested fields limit if mapping is not updated
        Settings settings = Settings.builder().put(MapperService.INDEX_MAPPING_NESTED_FIELDS_LIMIT_SETTING.getKey(), 0).build();
        MapperService mapperService = createMapperService(settings, mapping(b -> {}));
        merge(mapperService, MergeReason.MAPPING_RECOVERY, mapping.apply("_doc"));
    }

    public void testLimitNestedDocsDefaultSettings() throws Exception {
        Settings settings = Settings.builder().build();
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> b.startObject("nested1").field("type", "nested").endObject()));

        long defaultMaxNoNestedDocs = MapperService.INDEX_MAPPING_NESTED_DOCS_LIMIT_SETTING.get(settings);

        // parsing a doc with No. nested objects > defaultMaxNoNestedDocs fails
        XContentBuilder docBuilder = XContentFactory.jsonBuilder();
        docBuilder.startObject();
        {
            docBuilder.startArray("nested1");
            {
                for (int i = 0; i <= defaultMaxNoNestedDocs; i++) {
                    docBuilder.startObject().field("f", i).endObject();
                }
            }
            docBuilder.endArray();
        }
        docBuilder.endObject();
        SourceToParse source1 = new SourceToParse("1", BytesReference.bytes(docBuilder), XContentType.JSON);
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> docMapper.parse(source1));
        assertEquals(
            "The number of nested documents has exceeded the allowed limit of ["
                + defaultMaxNoNestedDocs
                + "]. This limit can be set by changing the ["
                + MapperService.INDEX_MAPPING_NESTED_DOCS_LIMIT_SETTING.getKey()
                + "] index level setting.",
            e.getMessage()
        );
    }

    public void testLimitNestedDocs() throws Exception {
        // setting limit to allow only two nested objects in the whole doc
        long maxNoNestedDocs = 2L;
        Settings settings = Settings.builder().put(MapperService.INDEX_MAPPING_NESTED_DOCS_LIMIT_SETTING.getKey(), maxNoNestedDocs).build();
        DocumentMapper docMapper = createMapperService(settings, mapping(b -> b.startObject("nested1").field("type", "nested").endObject()))
            .documentMapper();

        // parsing a doc with 2 nested objects succeeds
        XContentBuilder docBuilder = XContentFactory.jsonBuilder();
        docBuilder.startObject();
        {
            docBuilder.startArray("nested1");
            {
                docBuilder.startObject().field("field1", "11").field("field2", "21").endObject();
                docBuilder.startObject().field("field1", "12").field("field2", "22").endObject();
            }
            docBuilder.endArray();
        }
        docBuilder.endObject();
        SourceToParse source1 = new SourceToParse("1", BytesReference.bytes(docBuilder), XContentType.JSON);
        ParsedDocument doc = docMapper.parse(source1);
        assertThat(doc.docs().size(), equalTo(3));

        // parsing a doc with 3 nested objects fails
        XContentBuilder docBuilder2 = XContentFactory.jsonBuilder();
        docBuilder2.startObject();
        {
            docBuilder2.startArray("nested1");
            {
                docBuilder2.startObject().field("field1", "11").field("field2", "21").endObject();
                docBuilder2.startObject().field("field1", "12").field("field2", "22").endObject();
                docBuilder2.startObject().field("field1", "13").field("field2", "23").endObject();
            }
            docBuilder2.endArray();
        }
        docBuilder2.endObject();
        SourceToParse source2 = new SourceToParse("2", BytesReference.bytes(docBuilder2), XContentType.JSON);
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> docMapper.parse(source2));
        assertEquals(
            "The number of nested documents has exceeded the allowed limit of ["
                + maxNoNestedDocs
                + "]. This limit can be set by changing the ["
                + MapperService.INDEX_MAPPING_NESTED_DOCS_LIMIT_SETTING.getKey()
                + "] index level setting.",
            e.getMessage()
        );
    }

    public void testLimitNestedDocsMultipleNestedFields() throws Exception {
        // setting limit to allow only two nested objects in the whole doc
        long maxNoNestedDocs = 2L;
        Settings settings = Settings.builder().put(MapperService.INDEX_MAPPING_NESTED_DOCS_LIMIT_SETTING.getKey(), maxNoNestedDocs).build();
        DocumentMapper docMapper = createMapperService(settings, mapping(b -> {
            b.startObject("nested1").field("type", "nested").endObject();
            b.startObject("nested2").field("type", "nested").endObject();
        })).documentMapper();

        // parsing a doc with 2 nested objects succeeds
        XContentBuilder docBuilder = XContentFactory.jsonBuilder();
        docBuilder.startObject();
        {
            docBuilder.startArray("nested1");
            {
                docBuilder.startObject().field("field1", "11").field("field2", "21").endObject();
            }
            docBuilder.endArray();
            docBuilder.startArray("nested2");
            {
                docBuilder.startObject().field("field1", "21").field("field2", "22").endObject();
            }
            docBuilder.endArray();
        }
        docBuilder.endObject();
        SourceToParse source1 = new SourceToParse("1", BytesReference.bytes(docBuilder), XContentType.JSON);
        ParsedDocument doc = docMapper.parse(source1);
        assertThat(doc.docs().size(), equalTo(3));

        // parsing a doc with 3 nested objects fails
        XContentBuilder docBuilder2 = XContentFactory.jsonBuilder();
        docBuilder2.startObject();
        {
            docBuilder2.startArray("nested1");
            {
                docBuilder2.startObject().field("field1", "11").field("field2", "21").endObject();
            }
            docBuilder2.endArray();
            docBuilder2.startArray("nested2");
            {
                docBuilder2.startObject().field("field1", "12").field("field2", "22").endObject();
                docBuilder2.startObject().field("field1", "13").field("field2", "23").endObject();
            }
            docBuilder2.endArray();

        }
        docBuilder2.endObject();
        SourceToParse source2 = new SourceToParse("2", BytesReference.bytes(docBuilder2), XContentType.JSON);
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> docMapper.parse(source2));
        assertEquals(
            "The number of nested documents has exceeded the allowed limit of ["
                + maxNoNestedDocs
                + "]. This limit can be set by changing the ["
                + MapperService.INDEX_MAPPING_NESTED_DOCS_LIMIT_SETTING.getKey()
                + "] index level setting.",
            e.getMessage()
        );
    }

    public void testReorderParent() throws IOException {

        Version version = VersionUtils.randomIndexCompatibleVersion(random());

        DocumentMapper docMapper = createDocumentMapper(
            version,
            mapping(b -> b.startObject("nested1").field("type", "nested").endObject())
        );

        assertNotEquals(NestedLookup.EMPTY, docMapper.mappers().nestedLookup());
        ObjectMapper mapper = docMapper.mappers().objectMappers().get("nested1");
        assertThat(mapper, instanceOf(NestedObjectMapper.class));

        ParsedDocument doc = docMapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field", "value")
                        .startArray("nested1")
                        .startObject()
                        .field("field1", "1")
                        .field("field2", "2")
                        .endObject()
                        .startObject()
                        .field("field1", "3")
                        .field("field2", "4")
                        .endObject()
                        .endArray()
                        .endObject()
                ),
                XContentType.JSON
            )
        );

        assertThat(doc.docs().size(), equalTo(3));
        NestedObjectMapper nested1Mapper = (NestedObjectMapper) mapper;
        if (version.before(Version.V_8_0_0)) {
            assertThat(doc.docs().get(0).get("_type"), equalTo(nested1Mapper.nestedTypePath()));
        } else {
            assertThat(doc.docs().get(0).get(NestedPathFieldMapper.NAME), equalTo(nested1Mapper.nestedTypePath()));
        }
        assertThat(doc.docs().get(0).get("nested1.field1"), equalTo("1"));
        assertThat(doc.docs().get(0).get("nested1.field2"), equalTo("2"));
        assertThat(doc.docs().get(1).get("nested1.field1"), equalTo("3"));
        assertThat(doc.docs().get(1).get("nested1.field2"), equalTo("4"));
        assertThat(doc.docs().get(2).get("field"), equalTo("value"));
    }

    public void testMergeChildMappings() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("nested1");
            b.field("type", "nested");
            b.startObject("properties");
            b.startObject("field1").field("type", "keyword").endObject();
            b.startObject("field2").field("type", "keyword").endObject();
            b.startObject("nested2").field("type", "nested").field("include_in_root", true).endObject();
            b.endObject();
            b.endObject();
        }));

        merge(mapperService, mapping(b -> {
            b.startObject("nested1");
            b.field("type", "nested");
            b.startObject("properties");
            b.startObject("field2").field("type", "keyword").endObject();
            b.startObject("field3").field("type", "keyword").endObject();
            b.startObject("nested2").field("type", "nested").field("include_in_root", true).endObject();
            b.endObject();
            b.endObject();
        }));

        NestedObjectMapper nested1 = (NestedObjectMapper) mapperService.mappingLookup().objectMappers().get("nested1");
        assertThat(nested1.getChildren().values(), hasSize(4));

        NestedObjectMapper nested2 = (NestedObjectMapper) nested1.getChildren().get("nested2");
        assertTrue(nested2.isIncludeInRoot());
    }

    public void testMergeNestedMappings() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> b.startObject("nested1").field("type", "nested").endObject()));

        // cannot update `include_in_parent` dynamically
        MapperException e1 = expectThrows(MapperException.class, () -> merge(mapperService, mapping(b -> {
            b.startObject("nested1");
            {
                b.field("type", "nested");
                b.field("include_in_parent", true);
            }
            b.endObject();
        })));
        assertEquals("the [include_in_parent] parameter can't be updated on a nested object mapping", e1.getMessage());

        // cannot update `include_in_root` dynamically
        MapperException e2 = expectThrows(MapperException.class, () -> merge(mapperService, mapping(b -> {
            b.startObject("nested1");
            {
                b.field("type", "nested");
                b.field("include_in_root", true);
            }
            b.endObject();
        })));
        assertEquals("the [include_in_root] parameter can't be updated on a nested object mapping", e2.getMessage());
    }

    public void testEnabled() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("nested");
            b.field("type", "nested");
            b.field("enabled", "false");
            b.endObject();
        }));
        {
            NestedObjectMapper nom = (NestedObjectMapper) mapperService.mappingLookup().objectMappers().get("nested");
            assertFalse(nom.isEnabled());
        }

        merge(mapperService, mapping(b -> {
            b.startObject("nested");
            b.field("type", "nested");
            b.field("enabled", "false");
            b.endObject();
        }));
        {
            NestedObjectMapper nom = (NestedObjectMapper) mapperService.mappingLookup().objectMappers().get("nested");
            assertFalse(nom.isEnabled());
        }

        // merging for index templates allows override of 'enabled' param
        merge(mapperService, MergeReason.INDEX_TEMPLATE, mapping(b -> {
            b.startObject("nested");
            b.field("type", "nested");
            b.field("enabled", "true");
            b.endObject();
        }));
        {
            NestedObjectMapper nom = (NestedObjectMapper) mapperService.mappingLookup().objectMappers().get("nested");
            assertTrue(nom.isEnabled());
        }

        // but a normal merge does not permit 'enabled' overrides
        Exception e = expectThrows(MapperException.class, () -> merge(mapperService, mapping(b -> {
            b.startObject("nested");
            b.field("type", "nested");
            b.field("enabled", "false");
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("the [enabled] parameter can't be updated for the object mapping [nested]"));
    }

    public void testMergeNestedMappingsFromDynamicUpdate() throws IOException {

        // Check that dynamic mappings have redundant includes removed

        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startArray("dynamic_templates");
            b.startObject();
            b.startObject("object_fields");
            b.field("match_mapping_type", "object");
            b.startObject("mapping");
            b.field("type", "nested");
            b.field("include_in_parent", true);
            b.field("include_in_root", true);
            b.endObject();
            b.field("match", "*");
            b.endObject();
            b.endObject();
            b.endArray();
        }));

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.startObject("object").endObject()));

        merge(mapperService, Strings.toString(doc.dynamicMappingsUpdate()));
        merge(mapperService, Strings.toString(doc.dynamicMappingsUpdate()));

        assertThat(Strings.toString(mapperService.documentMapper().mapping()), containsString("""
            "properties":{"object":{"type":"nested","include_in_parent":true}}"""));
    }

    public void testFieldNames() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {
            b.startObject("nested1");
            {
                b.field("type", "nested");
                b.startObject("properties");
                {
                    b.startObject("integer1").field("type", "integer").field("doc_values", true).endObject();
                    b.startObject("nested2");
                    {
                        b.field("type", "nested");
                        b.startObject("properties");
                        {
                            b.startObject("integer2").field("type", "integer").field("doc_values", false).endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        XContentBuilder b = XContentFactory.jsonBuilder();
        b.startObject();
        b.array("nested1", nested1 -> { // doc 6
            nested1.startObject(); // doc 0
            {
                nested1.field("integer1", 1);
            }
            nested1.endObject();
            nested1.startObject(); // doc 2
            {
                nested1.field("integer1", 11);
                nested1.array("nested2", nested2 -> {
                    nested2.startObject().endObject(); // doc 1
                });
            }
            nested1.endObject();
            nested1.startObject(); // doc 5
            {
                nested1.field("integer1", 21);
                nested1.array("nested2", nested2 -> {
                    nested2.startObject(); // doc 3
                    {
                        nested1.field("integer2", 22);
                    }
                    nested1.endObject();
                    nested2.startObject().endObject(); // doc 4
                });
            }
            nested1.endObject();
        });
        b.endObject();
        ParsedDocument doc = docMapper.parse(new SourceToParse("1", BytesReference.bytes(b), XContentType.JSON));

        // Note doc values are disabled for field "integer2",
        // so the document only contains an IntPoint field whose stringValue method always returns null.
        // Thus so we cannot use get() for this field, we must use getNumericValue().
        assertThat(doc.docs().size(), equalTo(7));
        // Only fields without doc values are added to field names.
        assertThat(doc.docs().get(6).get("_field_names"), nullValue());
        assertThat(doc.docs().get(0).get("nested1.integer1"), equalTo("1"));
        assertThat(doc.docs().get(0).get("nested1._field_names"), nullValue());
        assertThat(doc.docs().get(2).get("nested1.integer1"), equalTo("11"));
        assertThat(doc.docs().get(2).get("_field_names"), nullValue());
        assertThat(doc.docs().get(1).getNumericValue("nested1.nested2.integer2"), nullValue());
        assertThat(doc.docs().get(1).get("_field_names"), nullValue());
        assertThat(doc.docs().get(5).get("nested1.integer1"), equalTo("21"));
        assertThat(doc.docs().get(5).get("_field_names"), nullValue());
        assertThat(doc.docs().get(3).getNumericValue("nested1.nested2.integer2"), equalTo(22));
        assertThat(doc.docs().get(3).get("_field_names"), equalTo("nested1.nested2.integer2"));
        assertThat(doc.docs().get(4).getNumericValue("nested1.nested2.integer2"), nullValue());
        assertThat(doc.docs().get(4).get("_field_names"), nullValue());
    }

    public void testFieldNamesIncludeInParent() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {
            b.startObject("nested1");
            {
                b.field("type", "nested");
                b.startObject("properties");
                {
                    b.startObject("integer1").field("type", "integer").field("doc_values", true).endObject();
                    b.startObject("nested2");
                    {
                        b.field("type", "nested");
                        b.field("include_in_parent", true);
                        b.startObject("properties");
                        {
                            b.startObject("integer2").field("type", "integer").field("doc_values", false).endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        XContentBuilder b = XContentFactory.jsonBuilder();
        b.startObject();
        b.array("nested1", nested1 -> { // doc 6
            nested1.startObject(); // doc 0
            {
                nested1.field("integer1", 1);
            }
            nested1.endObject();
            nested1.startObject(); // doc 2
            {
                nested1.field("integer1", 11);
                nested1.array("nested2", nested2 -> {
                    nested2.startObject().endObject(); // doc 1
                });
            }
            nested1.endObject();
            nested1.startObject(); // doc 5
            {
                nested1.field("integer1", 21);
                nested1.array("nested2", nested2 -> {
                    nested2.startObject(); // doc 3
                    {
                        nested1.field("integer2", 22);
                    }
                    nested1.endObject();
                    nested2.startObject().endObject(); // doc 4
                });
            }
            nested1.endObject();
        });
        b.endObject();
        ParsedDocument doc = docMapper.parse(new SourceToParse("1", BytesReference.bytes(b), XContentType.JSON));

        // Note doc values are disabled for field "integer2",
        // so the document only contains an IntPoint field whose stringValue method always returns null.
        // Thus so we cannot use get() for this field, we must use getNumericValue().
        assertThat(doc.docs().size(), equalTo(7));
        // Only fields without doc values are added to field names.
        assertThat(doc.docs().get(6).get("_field_names"), nullValue());
        assertThat(doc.docs().get(0).get("nested1.integer1"), equalTo("1"));
        assertThat(doc.docs().get(0).get("nested1._field_names"), nullValue());
        assertThat(doc.docs().get(2).get("nested1.integer1"), equalTo("11"));
        assertThat(doc.docs().get(2).get("_field_names"), nullValue());
        assertThat(doc.docs().get(1).getNumericValue("nested1.nested2.integer2"), nullValue());
        assertThat(doc.docs().get(1).get("_field_names"), nullValue());
        assertThat(doc.docs().get(5).get("nested1.integer1"), equalTo("21"));
        assertThat(doc.docs().get(5).getNumericValue("nested1.nested2.integer2"), equalTo(22));
        assertThat(doc.docs().get(5).get("_field_names"), equalTo("nested1.nested2.integer2"));
        assertThat(doc.docs().get(3).getNumericValue("nested1.nested2.integer2"), equalTo(22));
        assertThat(doc.docs().get(3).get("_field_names"), equalTo("nested1.nested2.integer2"));
        assertThat(doc.docs().get(4).getNumericValue("nested1.nested2.integer2"), nullValue());
        assertThat(doc.docs().get(4).get("_field_names"), nullValue());
    }

    public void testFieldNamesIncludeInRoot() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {
            b.startObject("nested1");
            {
                b.field("type", "nested");
                b.startObject("properties");
                {
                    b.startObject("integer1").field("type", "integer").field("doc_values", true).endObject();
                    b.startObject("nested2");
                    {
                        b.field("type", "nested");
                        b.field("include_in_root", true);
                        b.startObject("properties");
                        {
                            b.startObject("integer2").field("type", "integer").field("doc_values", false).endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        XContentBuilder b = XContentFactory.jsonBuilder();
        b.startObject();
        b.array("nested1", nested1 -> { // doc 6
            nested1.startObject(); // doc 0
            {
                nested1.field("integer1", 1);
            }
            nested1.endObject();
            nested1.startObject(); // doc 2
            {
                nested1.field("integer1", 11);
                nested1.array("nested2", nested2 -> {
                    nested2.startObject().endObject(); // doc 1
                });
            }
            nested1.endObject();
            nested1.startObject(); // doc 5
            {
                nested1.field("integer1", 21);
                nested1.array("nested2", nested2 -> {
                    nested2.startObject(); // doc 3
                    {
                        nested1.field("integer2", 22);
                    }
                    nested1.endObject();
                    nested2.startObject().endObject(); // doc 4
                });
            }
            nested1.endObject();
        });
        b.endObject();
        ParsedDocument doc = docMapper.parse(new SourceToParse("1", BytesReference.bytes(b), XContentType.JSON));

        // Note doc values are disabled for field "integer2",
        // so the document only contains an IntPoint field whose stringValue method always returns null.
        // Thus so we cannot use get() for this field, we must use getNumericValue().
        assertThat(doc.docs().size(), equalTo(7));
        // Only fields without doc values are added to field names.
        assertThat(doc.docs().get(6).getNumericValue("nested1.nested2.integer2"), equalTo(22));
        assertThat(doc.docs().get(6).get("_field_names"), equalTo("nested1.nested2.integer2"));
        assertThat(doc.docs().get(0).get("nested1.integer1"), equalTo("1"));
        assertThat(doc.docs().get(0).get("nested1._field_names"), nullValue());
        assertThat(doc.docs().get(2).get("nested1.integer1"), equalTo("11"));
        assertThat(doc.docs().get(2).get("_field_names"), nullValue());
        assertThat(doc.docs().get(1).getNumericValue("nested1.nested2.integer2"), nullValue());
        assertThat(doc.docs().get(1).get("_field_names"), nullValue());
        assertThat(doc.docs().get(5).get("nested1.integer1"), equalTo("21"));
        assertThat(doc.docs().get(5).get("_field_names"), nullValue());
        assertThat(doc.docs().get(3).getNumericValue("nested1.nested2.integer2"), equalTo(22));
        assertThat(doc.docs().get(3).get("_field_names"), equalTo("nested1.nested2.integer2"));
        assertThat(doc.docs().get(4).getNumericValue("nested1.nested2.integer2"), nullValue());
        assertThat(doc.docs().get(4).get("_field_names"), nullValue());
    }

    public void testNoDimensionNestedFields() {
        {
            Exception e = expectThrows(IllegalArgumentException.class, () -> createDocumentMapper(mapping(b -> {
                b.startObject("nested");
                {
                    b.field("type", "nested");
                    b.startObject("properties");
                    {
                        b.startObject("foo")
                            .field("type", randomFrom(List.of("keyword", "ip", "long", "short", "integer", "byte")))
                            .field("time_series_dimension", true)
                            .endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            })));
            assertThat(e.getMessage(), containsString("time_series_dimension can't be configured in nested field [nested.foo]"));
        }

        {
            Exception e = expectThrows(IllegalArgumentException.class, () -> createDocumentMapper(mapping(b -> {
                b.startObject("nested");
                {
                    b.field("type", "nested");
                    b.startObject("properties");
                    {
                        b.startObject("other").field("type", "keyword").endObject();
                        b.startObject("object").field("type", "object");
                        {
                            b.startObject("properties");
                            {
                                b.startObject("foo")
                                    .field("type", randomFrom(List.of("keyword", "ip", "long", "short", "integer", "byte")))
                                    .field("time_series_dimension", true)
                                    .endObject();
                            }
                            b.endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            })));
            assertThat(e.getMessage(), containsString("time_series_dimension can't be configured in nested field [nested.object.foo]"));
        }
    }
}
