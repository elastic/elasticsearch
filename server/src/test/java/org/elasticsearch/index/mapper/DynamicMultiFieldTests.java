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
import org.elasticsearch.index.mapper.MapperService.MergeReason;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class DynamicMultiFieldTests extends MapperServiceTestCase {

    private static FieldMapper.Builder keyword(MapperService mapperService, String name) {
        MappingParserContext parserContext = mapperService.parserContext();
        return (FieldMapper.Builder) parserContext.typeParser("keyword").parse(name, new HashMap<>(), parserContext);
    }

    /** The builders the context collected for {@code parent}, which is what the dynamic update is made of. */
    private static List<Mapper.Builder> addMultiField(MapperService mapperService, String parent, String multiField) {
        DocumentParserContext context = new TestDocumentParserContext(mapperService.parserContext(), mapperService.mappingLookup());
        FieldMapper.Builder sub = keyword(mapperService, multiField);
        context.updateMapping(parent, builder -> ((FieldMapper.Builder) builder).addMultiField(sub));
        return context.getDynamicMappers(parent);
    }

    public void testUpdateRestatesTheParent() throws IOException {
        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", "text").field("store", true).startObject("meta").field("a", "b").endObject())
        );

        List<Mapper.Builder> added = addMultiField(mapperService, "field", "raw");

        assertThat(added, hasSize(1));
        // everything of the parent's that is mapped is in the update, so merging it can not reset any of it
        assertThat(Strings.toString(added.get(0)), equalTo("""
            {"field":{"type":"text","store":true,"fields":{"raw":{"type":"keyword"}},"meta":{"a":"b"}}}"""));
    }

    public void testMergingTheUpdateAddsTheSubFieldAndKeepsTheParentsParameters() throws IOException {
        MapperService mapperService = createMapperService(
            fieldMapping(
                b -> b.field("type", "text")
                    .field("store", true)
                    .startObject("meta")
                    .field("a", "b")
                    .endObject()
                    .startObject("fields")
                    .startObject("existing")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
            )
        );

        List<Mapper.Builder> added = addMultiField(mapperService, "field", "raw");
        merge(mapperService, MergeReason.MAPPING_AUTO_UPDATE, "{\"_doc\":{\"properties\":" + Strings.toString(added.get(0)) + "}}");

        String source = mapperService.documentMapper().mappingSource().string();
        assertThat(source, containsString("\"store\":true"));
        assertThat(source, containsString("\"meta\":{\"a\":\"b\"}"));
        assertThat(source, containsString("\"existing\":{\"type\":\"keyword\"}"));
        assertThat(source, containsString("\"raw\":{\"type\":\"keyword\"}"));
    }

    public void testParentInsideAnObject() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("obj").startObject("properties");
            b.startObject("field").field("type", "text").endObject();
            b.endObject().endObject();
        }));

        List<Mapper.Builder> added = addMultiField(mapperService, "obj.field", "raw");

        assertThat(added, hasSize(1));
        assertThat(added.get(0).leafName(), equalTo("field"));
        assertThat(Strings.toString(added.get(0)), equalTo("""
            {"field":{"type":"text","fields":{"raw":{"type":"keyword"}}}}"""));
    }

    /** With {@code subobjects: false} the leaf name of a field is its path below the root, dots included. */
    public void testParentWithDotsInItsLeafName() throws IOException {
        MapperService mapperService = createMapperService(
            mappingNoSubobjects(b -> b.startObject("obj.my.field").field("type", "text").endObject())
        );

        List<Mapper.Builder> added = addMultiField(mapperService, "obj.my.field", "raw");

        assertThat(added, hasSize(1));
        assertThat(added.get(0).leafName(), equalTo("obj.my.field"));
    }

    public void testUnmappedParent() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "text")));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> addMultiField(mapperService, "missing", "raw"));
        assertThat(e.getMessage(), containsString("[missing], which is not a mapped field"));
    }
}
