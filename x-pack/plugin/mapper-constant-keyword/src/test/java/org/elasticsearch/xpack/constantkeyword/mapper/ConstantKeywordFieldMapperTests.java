/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.constantkeyword.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMapperTestCase;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.xpack.constantkeyword.ConstantKeywordMapperPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class ConstantKeywordFieldMapperTests extends FieldMapperTestCase<ConstantKeywordFieldMapper.Builder> {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(ConstantKeywordMapperPlugin.class, LocalStateCompositeXPackPlugin.class);
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
            a.setValue("foo");;
        });
        addModifier("value-from-null", true, (a, b) -> {
            b.setValue("bar");
        });
    }

    public void testDefaults() throws Exception {
        IndexService indexService = createIndex("test");
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties").startObject("field").field("type", "constant_keyword")
                .field("value", "foo").endObject().endObject().endObject().endObject());
        DocumentMapper mapper = indexService.mapperService().merge("_doc", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping, mapper.mappingSource().toString());

        BytesReference source = BytesReference.bytes(XContentFactory.jsonBuilder().startObject().endObject());
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", source, XContentType.JSON));
        assertNull(doc.rootDoc().getField("field"));

        source = BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", "foo").endObject());
        doc = mapper.parse(new SourceToParse("test", "1", source, XContentType.JSON));
        assertNull(doc.rootDoc().getField("field"));

        BytesReference illegalSource = BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("field", "bar").endObject());
        MapperParsingException e = expectThrows(MapperParsingException.class,
                () -> mapper.parse(new SourceToParse("test", "1", illegalSource, XContentType.JSON)));
        assertEquals("[constant_keyword] field [field] only accepts values that are equal to the value defined in the mappings [foo], " +
                "but got [bar]", e.getCause().getMessage());
    }

    public void testDynamicValue() throws Exception {
        IndexService indexService = createIndex("test");
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties").startObject("field").field("type", "constant_keyword")
                .endObject().endObject().endObject().endObject());
        DocumentMapper mapper = indexService.mapperService().merge("_doc", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping, mapper.mappingSource().toString());

        BytesReference source = BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", "foo").endObject());
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", source, XContentType.JSON));
        assertNull(doc.rootDoc().getField("field"));
        assertNotNull(doc.dynamicMappingsUpdate());

        CompressedXContent mappingUpdate = new CompressedXContent(Strings.toString(doc.dynamicMappingsUpdate()));
        DocumentMapper updatedMapper = indexService.mapperService().merge("_doc", mappingUpdate, MergeReason.MAPPING_UPDATE);
        String expectedMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties").startObject("field").field("type", "constant_keyword")
                .field("value", "foo").endObject().endObject().endObject().endObject());
        assertEquals(expectedMapping, updatedMapper.mappingSource().toString());

        doc = updatedMapper.parse(new SourceToParse("test", "1", source, XContentType.JSON));
        assertNull(doc.rootDoc().getField("field"));
        assertNull(doc.dynamicMappingsUpdate());
    }

    public void testMeta() throws Exception {
        IndexService indexService = createIndex("test");
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties").startObject("field").field("type", "constant_keyword")
                .field("meta", Collections.singletonMap("foo", "bar"))
                .endObject().endObject().endObject().endObject());

        DocumentMapper mapper = indexService.mapperService().merge("_doc",
                new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping, mapper.mappingSource().toString());

        String mapping2 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties").startObject("field").field("type", "constant_keyword")
                .endObject().endObject().endObject().endObject());
        mapper = indexService.mapperService().merge("_doc",
                new CompressedXContent(mapping2), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping2, mapper.mappingSource().toString());

        String mapping3 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties").startObject("field").field("type", "constant_keyword")
                .field("meta", Collections.singletonMap("baz", "quux"))
                .endObject().endObject().endObject().endObject());
        mapper = indexService.mapperService().merge("_doc",
                new CompressedXContent(mapping3), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping3, mapper.mappingSource().toString());
    }

    public void testLookupValues() throws Exception {
        IndexService indexService = createIndex("test");
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties").startObject("field").field("type", "constant_keyword")
                .endObject().endObject().endObject().endObject());
        DocumentMapper mapper = indexService.mapperService().merge("_doc", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping, mapper.mappingSource().toString());

        FieldMapper fieldMapper = (FieldMapper) mapper.mappers().getMapper("field");
        List<?> values = fieldMapper.lookupValues(new SourceLookup(), null);
        assertTrue(values.isEmpty());

        String mapping2 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties").startObject("field").field("type", "constant_keyword")
                .field("value", "foo").endObject().endObject().endObject().endObject());
        mapper = indexService.mapperService().merge("_doc", new CompressedXContent(mapping2), MergeReason.MAPPING_UPDATE);

        fieldMapper = (FieldMapper) mapper.mappers().getMapper("field");
        values = fieldMapper.lookupValues(new SourceLookup(), null);
        assertEquals(1, values.size());
        assertEquals("foo", values.get(0));
    }
}
