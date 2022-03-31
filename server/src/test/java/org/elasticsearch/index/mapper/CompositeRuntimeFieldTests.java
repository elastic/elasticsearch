/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.script.CompositeFieldScript;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class CompositeRuntimeFieldTests extends MapperServiceTestCase {

    @Override
    @SuppressWarnings("unchecked")
    protected <T> T compileScript(Script script, ScriptContext<T> context) {
        if (context == CompositeFieldScript.CONTEXT) {
            return (T) (CompositeFieldScript.Factory) (fieldName, params, searchLookup) -> ctx -> new CompositeFieldScript(
                fieldName,
                params,
                searchLookup,
                ctx
            ) {
                @Override
                public void execute() {
                    if (script.getIdOrCode().equals("split-str-long")) {
                        List<Object> values = extractFromSource("field");
                        String input = values.get(0).toString();
                        String[] parts = input.split(" ");
                        emit("str", parts[0]);
                        emit("long", parts[1]);
                    }
                }
            };
        }
        if (context == LongFieldScript.CONTEXT) {
            return (T) (LongFieldScript.Factory) (field, params, lookup) -> ctx -> new LongFieldScript(field, params, lookup, ctx) {
                @Override
                public void execute() {

                }
            };
        }
        throw new UnsupportedOperationException("Unknown context " + context.name);
    }

    public void testObjectDefinition() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startObject("runtime");
            b.startObject("obj");
            b.field("type", "composite");
            b.startObject("script").field("source", "dummy").endObject();
            b.startObject("fields");
            b.startObject("long-subfield").field("type", "long").endObject();
            b.startObject("str-subfield").field("type", "keyword").endObject();
            b.startObject("double-subfield").field("type", "double").endObject();
            b.startObject("boolean-subfield").field("type", "boolean").endObject();
            b.startObject("ip-subfield").field("type", "ip").endObject();
            b.startObject("geopoint-subfield").field("type", "geo_point").endObject();
            b.endObject();
            b.endObject();
            b.endObject();
        }));

        assertNull(mapperService.mappingLookup().getFieldType("obj"));
        assertNull(mapperService.mappingLookup().getFieldType("long-subfield"));
        assertNull(mapperService.mappingLookup().getFieldType("str-subfield"));
        assertNull(mapperService.mappingLookup().getFieldType("double-subfield"));
        assertNull(mapperService.mappingLookup().getFieldType("boolean-subfield"));
        assertNull(mapperService.mappingLookup().getFieldType("ip-subfield"));
        assertNull(mapperService.mappingLookup().getFieldType("geopoint-subfield"));
        assertNull(mapperService.mappingLookup().getFieldType("obj.any-subfield"));
        MappedFieldType longSubfield = mapperService.mappingLookup().getFieldType("obj.long-subfield");
        assertEquals("obj.long-subfield", longSubfield.name());
        assertEquals("long", longSubfield.typeName());
        MappedFieldType strSubfield = mapperService.mappingLookup().getFieldType("obj.str-subfield");
        assertEquals("obj.str-subfield", strSubfield.name());
        assertEquals("keyword", strSubfield.typeName());
        MappedFieldType doubleSubfield = mapperService.mappingLookup().getFieldType("obj.double-subfield");
        assertEquals("obj.double-subfield", doubleSubfield.name());
        assertEquals("double", doubleSubfield.typeName());
        MappedFieldType booleanSubfield = mapperService.mappingLookup().getFieldType("obj.boolean-subfield");
        assertEquals("obj.boolean-subfield", booleanSubfield.name());
        assertEquals("boolean", booleanSubfield.typeName());
        MappedFieldType ipSubfield = mapperService.mappingLookup().getFieldType("obj.ip-subfield");
        assertEquals("obj.ip-subfield", ipSubfield.name());
        assertEquals("ip", ipSubfield.typeName());
        MappedFieldType geoPointSubfield = mapperService.mappingLookup().getFieldType("obj.geopoint-subfield");
        assertEquals("obj.geopoint-subfield", geoPointSubfield.name());
        assertEquals("geo_point", geoPointSubfield.typeName());

        RuntimeField rf = mapperService.mappingLookup().getMapping().getRoot().getRuntimeField("obj");
        assertEquals("obj", rf.name());
        Collection<MappedFieldType> mappedFieldTypes = rf.asMappedFieldTypes().toList();
        for (MappedFieldType mappedFieldType : mappedFieldTypes) {
            if (mappedFieldType.name().equals("obj.long-subfield")) {
                assertSame(longSubfield, mappedFieldType);
            } else if (mappedFieldType.name().equals("obj.str-subfield")) {
                assertSame(strSubfield, mappedFieldType);
            } else if (mappedFieldType.name().equals("obj.double-subfield")) {
                assertSame(doubleSubfield, mappedFieldType);
            } else if (mappedFieldType.name().equals("obj.boolean-subfield")) {
                assertSame(booleanSubfield, mappedFieldType);
            } else if (mappedFieldType.name().equals("obj.ip-subfield")) {
                assertSame(ipSubfield, mappedFieldType);
            } else if (mappedFieldType.name().equals("obj.geopoint-subfield")) {
                assertSame(geoPointSubfield, mappedFieldType);
            } else {
                fail("unexpected subfield [" + mappedFieldType.name() + "]");
            }
        }
    }

    public void testUnsupportedLeafType() {
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(topMapping(b -> {
            b.startObject("runtime");
            b.startObject("obj");
            b.field("type", "composite");
            b.startObject("script").field("source", "dummy").endObject();
            b.startObject("fields");
            b.startObject("long-subfield").field("type", "unsupported").endObject();
            b.endObject();
            b.endObject();
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString(""));
    }

    public void testToXContent() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startObject("runtime");
            b.startObject("message");
            b.field("type", "composite");
            b.field("script", "dummy");
            b.startObject("meta").field("test-meta", "value").endObject();
            b.startObject("fields").startObject("response").field("type", "long").endObject().endObject();
            b.endObject();
            b.endObject();
        }));
        assertEquals(
            """
                {"_doc":{"runtime":{"message":{"type":"composite","meta":{"test-meta":"value"},\
                "script":{"source":"dummy","lang":"painless"},"fields":{"response":{"type":"long"}}}}}}""",
            Strings.toString(mapperService.mappingLookup().getMapping())
        );
    }

    public void testScriptOnSubFieldThrowsError() {
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(runtimeMapping(b -> {
            b.startObject("obj");
            b.field("type", "composite");
            b.field("script", "dummy");
            b.startObject("fields");
            b.startObject("long").field("type", "long").field("script", "dummy").endObject();
            b.endObject();
            b.endObject();
        })));

        assertThat(e.getMessage(), containsString("Cannot use [script] parameter on sub-field [long] of composite field [obj]"));
    }

    public void testObjectWithoutScript() {
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(runtimeMapping(b -> {
            b.startObject("obj");
            b.field("type", "composite");
            b.startObject("fields");
            b.startObject("long").field("type", "long").endObject();
            b.endObject();
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("composite runtime field [obj] must declare a [script]"));
    }

    public void testObjectNullScript() {
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(runtimeMapping(b -> {
            b.startObject("obj");
            b.field("type", "composite");
            b.nullField("script");
            b.startObject("fields");
            b.startObject("long").field("type", "long").endObject();
            b.endObject();
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString(" [script] on runtime field [obj] of type [composite] must not have a [null] value"));

    }

    public void testObjectWithoutFields() {
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(runtimeMapping(b -> {
                b.startObject("obj");
                b.field("type", "composite");
                b.field("script", "dummy");
                b.endObject();
            })));
            assertThat(e.getMessage(), containsString("composite runtime field [obj] must declare its [fields]"));
        }
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(runtimeMapping(b -> {
                b.startObject("obj");
                b.field("type", "composite");
                b.field("script", "dummy");
                b.startObject("fields").endObject();
                b.endObject();
            })));
            assertThat(e.getMessage(), containsString("composite runtime field [obj] must declare its [fields]"));
        }
    }

    public void testMappingUpdate() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startObject("runtime");
            b.startObject("obj");
            b.field("type", "composite");
            b.startObject("script").field("source", "dummy").endObject();
            b.startObject("fields");
            b.startObject("long-subfield").field("type", "long").endObject();
            b.startObject("str-subfield").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
            b.endObject();
        }));

        XContentBuilder b = XContentBuilder.builder(XContentType.JSON.xContent());
        b.startObject();
        b.startObject("_doc");
        b.startObject("runtime");
        b.startObject("obj");
        b.field("type", "composite");
        b.startObject("script").field("source", "dummy2").endObject();
        b.startObject("fields");
        b.startObject("double-subfield").field("type", "double").endObject();
        b.endObject();
        b.endObject();
        b.endObject();
        b.endObject();
        b.endObject();

        merge(mapperService, b);

        assertNull(mapperService.mappingLookup().getFieldType("obj.long-subfield"));
        assertNull(mapperService.mappingLookup().getFieldType("obj.str-subfield"));
        MappedFieldType doubleSubField = mapperService.mappingLookup().getFieldType("obj.double-subfield");
        assertEquals("obj.double-subfield", doubleSubField.name());
        assertEquals("double", doubleSubField.typeName());

        RuntimeField rf = mapperService.mappingLookup().getMapping().getRoot().getRuntimeField("obj");
        assertEquals("obj", rf.name());

        Collection<MappedFieldType> mappedFieldTypes = rf.asMappedFieldTypes().toList();
        assertEquals(1, mappedFieldTypes.size());
        assertSame(doubleSubField, mappedFieldTypes.iterator().next());

        assertEquals("""
            {"obj":{"type":"composite","script":{"source":"dummy2","lang":"painless"},\
            "fields":{"double-subfield":{"type":"double"}}}}""", Strings.toString(rf));
    }

    public void testFieldDefinedTwiceWithSameName() throws IOException {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> createMapperService(topMapping(b -> {
            b.startObject("runtime");
            b.startObject("obj.long-subfield").field("type", "long").endObject();
            b.startObject("obj");
            b.field("type", "composite");
            b.startObject("script").field("source", "dummy").endObject();
            b.startObject("fields");
            b.startObject("long-subfield").field("type", "long").endObject();
            b.endObject();
            b.endObject();
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("Found two runtime fields with same name [obj.long-subfield]"));

        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startObject("runtime");
            b.startObject("obj.str-subfield").field("type", "long").endObject();
            b.startObject("obj");
            b.field("type", "composite");
            b.startObject("script").field("source", "dummy").endObject();
            b.startObject("fields");
            b.startObject("long-subfield").field("type", "long").endObject();
            b.endObject();
            b.endObject();
            b.endObject();
        }));

        XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
        builder.startObject();
        builder.startObject("_doc");
        builder.startObject("runtime");
        builder.startObject("obj.long-subfield").field("type", "long").endObject();
        builder.endObject();
        builder.endObject();
        builder.endObject();
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> merge(mapperService, builder));
        assertThat(iae.getMessage(), containsString("Found two runtime fields with same name [obj.long-subfield]"));
    }

    public void testParseDocumentSubFieldAccess() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.field("dynamic", false);
            b.startObject("runtime");
            b.startObject("obj");
            b.field("type", "composite");
            b.field("script", "split-str-long");
            b.startObject("fields");
            b.startObject("str").field("type", "keyword").endObject();
            b.startObject("long").field("type", "long").endObject();
            b.endObject();
            b.endObject();
            b.endObject();
        }));

        ParsedDocument doc1 = mapperService.documentMapper().parse(source(b -> b.field("field", "foo 1")));
        ParsedDocument doc2 = mapperService.documentMapper().parse(source(b -> b.field("field", "bar 2")));

        withLuceneIndex(mapperService, iw -> iw.addDocuments(Arrays.asList(doc1.rootDoc(), doc2.rootDoc())), reader -> {
            SearchLookup searchLookup = new SearchLookup(
                mapperService::fieldType,
                (mft, lookupSupplier) -> mft.fielddataBuilder("test", lookupSupplier).build(null, null)
            );

            LeafSearchLookup leafSearchLookup = searchLookup.getLeafSearchLookup(reader.leaves().get(0));

            leafSearchLookup.setDocument(0);
            assertEquals("foo", leafSearchLookup.doc().get("obj.str").get(0));
            assertEquals(1L, leafSearchLookup.doc().get("obj.long").get(0));

            leafSearchLookup.setDocument(1);
            assertEquals("bar", leafSearchLookup.doc().get("obj.str").get(0));
            assertEquals(2L, leafSearchLookup.doc().get("obj.long").get(0));
        });
    }

    public void testParseDocumentDynamicMapping() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startObject("runtime");
            b.startObject("obj");
            b.field("type", "composite");
            b.field("script", "dummy");
            b.startObject("fields");
            b.startObject("str").field("type", "keyword").endObject();
            b.startObject("long").field("type", "long").endObject();
            b.endObject();
            b.endObject();
            b.endObject();
        }));

        ParsedDocument doc1 = mapperService.documentMapper().parse(source(b -> b.field("obj.long", 1L).field("obj.str", "value")));
        assertNull(doc1.rootDoc().get("obj.long"));
        assertNull(doc1.rootDoc().get("obj.str"));

        assertNull(mapperService.mappingLookup().getMapper("obj.long"));
        assertNull(mapperService.mappingLookup().getMapper("obj.str"));
        assertNotNull(mapperService.mappingLookup().getFieldType("obj.long"));
        assertNotNull(mapperService.mappingLookup().getFieldType("obj.str"));

        XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
        builder.startObject();
        builder.startObject("_doc");
        builder.startObject("properties");
        builder.startObject("obj");
        builder.startObject("properties");
        builder.startObject("long").field("type", "long").endObject();
        builder.endObject();
        builder.endObject();
        builder.endObject();
        builder.endObject();
        builder.endObject();
        merge(mapperService, builder);

        ParsedDocument doc2 = mapperService.documentMapper().parse(source(b -> b.field("obj.long", 2L)));
        assertNotNull(doc2.rootDoc().get("obj.long"));
        assertNull(doc2.rootDoc().get("obj.str"));

        assertNotNull(mapperService.mappingLookup().getMapper("obj.long"));
        assertNull(mapperService.mappingLookup().getMapper("obj.str"));
        assertNotNull(mapperService.mappingLookup().getFieldType("obj.long"));
        assertNotNull(mapperService.mappingLookup().getFieldType("obj.str"));
    }

    public void testParseDocumentSubfieldsOutsideRuntimeObject() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startObject("runtime");
            b.startObject("obj");
            b.field("type", "composite");
            b.field("script", "dummy");
            b.startObject("fields");
            b.startObject("long").field("type", "long").endObject();
            b.endObject();
            b.endObject();
            b.endObject();
        }));

        ParsedDocument doc1 = mapperService.documentMapper().parse(source(b -> b.field("obj.long", 1L).field("obj.bool", true)));
        assertNull(doc1.rootDoc().get("obj.long"));
        assertNotNull(doc1.rootDoc().get("obj.bool"));

        assertEquals("""
            {"_doc":{"properties":{"obj":{"properties":{"bool":{"type":"boolean"}}}}}}""", Strings.toString(doc1.dynamicMappingsUpdate()));

        MapperService mapperService2 = createMapperService(topMapping(b -> {
            b.field("dynamic", "runtime");
            b.startObject("runtime");
            b.startObject("obj");
            b.field("type", "composite");
            b.field("script", "dummy");
            b.startObject("fields");
            b.startObject("long").field("type", "long").endObject();
            b.endObject();
            b.endObject();
            b.endObject();
        }));

        ParsedDocument doc2 = mapperService2.documentMapper().parse(source(b -> b.field("obj.long", 1L).field("obj.bool", true)));
        assertNull(doc2.rootDoc().get("obj.long"));
        assertNull(doc2.rootDoc().get("obj.bool"));
        assertEquals("""
            {"_doc":{"dynamic":"runtime","runtime":{"obj.bool":{"type":"boolean"}}}}""", Strings.toString(doc2.dynamicMappingsUpdate()));
    }
}
