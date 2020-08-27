/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.constantkeyword.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMapperTestCase2;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.xpack.constantkeyword.ConstantKeywordMapperPlugin;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class ConstantKeywordFieldMapperTests extends FieldMapperTestCase2<ConstantKeywordFieldMapper.Builder> {

    @Override
    protected Collection<Plugin> getPlugins() {
        return List.of(new ConstantKeywordMapperPlugin());
    }

    @Override
    protected Set<String> unsupportedProperties() {
        return Set.of("analyzer", "similarity", "store", "doc_values", "index");
    }

    @Override
    protected ConstantKeywordFieldMapper.Builder newBuilder() {
        return new ConstantKeywordFieldMapper.Builder("constant");
    }

    @Before
    public void addModifiers() {
        addModifier("value", false, (a, b) -> {
            a.setValue("foo");
            b.setValue("bar");
        });
        addModifier("unset", false, (a, b) -> {
            a.setValue("foo");
            ;
        });
        addModifier("value-from-null", true, (a, b) -> { b.setValue("bar"); });
    }

    public void testDefaults() throws Exception {
        XContentBuilder mapping = fieldMapping(b -> b.field("type", "constant_keyword").field("value", "foo"));
        DocumentMapper mapper = createDocumentMapper(mapping);
        assertEquals(Strings.toString(mapping), mapper.mappingSource().toString());

        BytesReference source = BytesReference.bytes(XContentFactory.jsonBuilder().startObject().endObject());
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", source, XContentType.JSON));
        assertNull(doc.rootDoc().getField("field"));

        source = BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", "foo").endObject());
        doc = mapper.parse(new SourceToParse("test", "1", source, XContentType.JSON));
        assertNull(doc.rootDoc().getField("field"));

        BytesReference illegalSource = BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", "bar").endObject());
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(new SourceToParse("test", "1", illegalSource, XContentType.JSON))
        );
        assertEquals(
            "[constant_keyword] field [field] only accepts values that are equal to the value defined in the mappings [foo], "
                + "but got [bar]",
            e.getCause().getMessage()
        );
    }

    public void testDynamicValue() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "constant_keyword")));

        BytesReference source = BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", "foo").endObject());
        ParsedDocument doc = mapperService.documentMapper().parse(new SourceToParse("test", "1", source, XContentType.JSON));
        assertNull(doc.rootDoc().getField("field"));
        assertNotNull(doc.dynamicMappingsUpdate());

        CompressedXContent mappingUpdate = new CompressedXContent(Strings.toString(doc.dynamicMappingsUpdate()));
        DocumentMapper updatedMapper = mapperService.merge("_doc", mappingUpdate, MergeReason.MAPPING_UPDATE);
        String expectedMapping = Strings.toString(fieldMapping(b -> b.field("type", "constant_keyword").field("value", "foo")));
        assertEquals(expectedMapping, updatedMapper.mappingSource().toString());

        doc = updatedMapper.parse(new SourceToParse("test", "1", source, XContentType.JSON));
        assertNull(doc.rootDoc().getField("field"));
        assertNull(doc.dynamicMappingsUpdate());
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "constant_keyword");
    }

    public void testFetchValue() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "constant_keyword")));
        FieldMapper fieldMapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        ValueFetcher fetcher = fieldMapper.valueFetcher(mapperService, null);

        SourceLookup missingValueLookup = new SourceLookup();
        SourceLookup nullValueLookup = new SourceLookup();
        nullValueLookup.setSource(Collections.singletonMap("field", null));

        assertTrue(fetcher.fetchValues(missingValueLookup).isEmpty());
        assertTrue(fetcher.fetchValues(nullValueLookup).isEmpty());

        merge(mapperService, fieldMapping(b -> b.field("type", "constant_keyword").field("value", "foo")));
        fieldMapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        fetcher = fieldMapper.valueFetcher(mapperService, null);

        assertEquals(List.of("foo"), fetcher.fetchValues(missingValueLookup));
        assertEquals(List.of("foo"), fetcher.fetchValues(nullValueLookup));
    }
}
