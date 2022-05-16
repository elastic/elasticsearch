/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.script.BooleanFieldScript;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class BooleanFieldMapperTests extends MapperTestCase {

    @Override
    protected Object getSampleValueForDocument() {
        return true;
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "boolean");
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("doc_values", b -> b.field("doc_values", false));
        checker.registerConflictCheck("index", b -> b.field("index", false));
        checker.registerConflictCheck("store", b -> b.field("store", true));
        checker.registerConflictCheck("null_value", b -> b.field("null_value", true));
    }

    public void testExistsQueryDocValuesDisabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", false);
        }));
        assertExistsQuery(mapperService);
        assertParseMinimalWarnings();
    }

    public void testDefaults() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapperService.documentMapper().parse(source(this::writeField));

        withLuceneIndex(mapperService, iw -> iw.addDocument(doc.rootDoc()), reader -> {
            final LeafReader leaf = reader.leaves().get(0).reader();
            // boolean fields are indexed and have doc values by default
            assertEquals(new BytesRef("T"), leaf.terms("field").iterator().next());
            SortedNumericDocValues values = leaf.getSortedNumericDocValues("field");
            assertNotNull(values);
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(1, values.nextValue());
        });
    }

    public void testSerialization() throws IOException {

        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        Mapper mapper = defaultMapper.mappers().getMapper("field");
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        mapper.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals("""
            {"field":{"type":"boolean"}}""", Strings.toString(builder));

        // now change some parameters
        defaultMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "boolean");
            b.field("doc_values", false);
            b.field("null_value", true);
        }));
        mapper = defaultMapper.mappers().getMapper("field");
        builder = XContentFactory.jsonBuilder().startObject();
        mapper.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals("""
            {"field":{"type":"boolean","doc_values":false,"null_value":true}}""", Strings.toString(builder));
    }

    public void testParsesBooleansStrict() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        // omit "false"/"true" here as they should still be parsed correctly
        for (String value : new String[] { "off", "no", "0", "on", "yes", "1" }) {
            MapperParsingException ex = expectThrows(
                MapperParsingException.class,
                () -> defaultMapper.parse(source(b -> b.field("field", value)))
            );
            assertEquals(
                "failed to parse field [field] of type [boolean] in document with id '1'. " + "Preview of field's value: '" + value + "'",
                ex.getMessage()
            );
        }
    }

    public void testParsesBooleansNestedStrict() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        MapperParsingException ex = expectThrows(MapperParsingException.class, () -> defaultMapper.parse(source(b -> {
            b.startObject("field");
            {
                b.field("inner_field", "no");
            }
            b.endObject();
        })));
        assertEquals(
            "failed to parse field [field] of type [boolean] in document with id '1'. " + "Preview of field's value: '{inner_field=no}'",
            ex.getMessage()
        );
    }

    public void testMultiFields() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "boolean");
            b.startObject("fields");
            {
                b.startObject("as_string").field("type", "keyword").endObject();
            }
            b.endObject();
        }));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", false)));
        assertNotNull(doc.rootDoc().getField("field.as_string"));
    }

    public void testDocValues() throws Exception {

        DocumentMapper defaultMapper = createDocumentMapper(mapping(b -> {
            b.startObject("bool1").field("type", "boolean").endObject();
            b.startObject("bool2");
            {
                b.field("type", "boolean");
                b.field("index", false);
            }
            b.endObject();
            b.startObject("bool3");
            {
                b.field("type", "boolean");
                b.field("index", true);
            }
            b.endObject();
        }));

        ParsedDocument parsedDoc = defaultMapper.parse(source(b -> {
            b.field("bool1", true);
            b.field("bool2", true);
            b.field("bool3", true);
        }));

        LuceneDocument doc = parsedDoc.rootDoc();
        IndexableField[] fields = doc.getFields("bool1");
        assertEquals(2, fields.length);
        assertEquals(DocValuesType.NONE, fields[0].fieldType().docValuesType());
        assertEquals(DocValuesType.SORTED_NUMERIC, fields[1].fieldType().docValuesType());
        fields = doc.getFields("bool2");
        assertEquals(1, fields.length);
        assertEquals(DocValuesType.SORTED_NUMERIC, fields[0].fieldType().docValuesType());
        fields = doc.getFields("bool3");
        assertEquals(DocValuesType.NONE, fields[0].fieldType().docValuesType());
        assertEquals(DocValuesType.SORTED_NUMERIC, fields[1].fieldType().docValuesType());
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        return switch (between(0, 3)) {
            case 0 -> randomBoolean();
            case 1 -> randomBoolean() ? "true" : "false";
            case 2 -> randomBoolean() ? "true" : "";
            case 3 -> randomBoolean() ? "true" : null;
            default -> throw new IllegalStateException();
        };
    }

    public void testScriptAndPrecludedParameters() {
        Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
            b.field("type", "boolean");
            b.field("script", "test");
            b.field("null_value", true);
        })));
        assertThat(e.getMessage(), equalTo("Failed to parse mapping: Field [null_value] cannot be set in conjunction with field [script]"));
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport() {
        return new SyntheticSourceSupport() {
            @Override
            public SyntheticSourceExample example() throws IOException {
                switch (randomInt(3)) {
                    case 0:
                        boolean v = randomBoolean();
                        return new SyntheticSourceExample(v, v, BooleanFieldMapperTests.this::minimalMapping);
                    case 1:
                        List<Boolean> in = randomList(1, 5, ESTestCase::randomBoolean);
                        Object out = in.size() == 1 ? in.get(0) : in.stream().sorted().toList();
                        return new SyntheticSourceExample(in, out, BooleanFieldMapperTests.this::minimalMapping);
                    case 2:
                        v = randomBoolean();
                        return new SyntheticSourceExample(null, v, b -> {
                            minimalMapping(b);
                            b.field("null_value", v);
                        });
                    case 3:
                        boolean nullValue = randomBoolean();
                        List<Boolean> vals = randomList(1, 5, ESTestCase::randomBoolean);
                        in = vals.stream().map(b -> b == nullValue ? null : b).toList();
                        out = vals.size() == 1 ? vals.get(0) : vals.stream().sorted().toList();
                        return new SyntheticSourceExample(in, out, b -> {
                            minimalMapping(b);
                            b.field("null_value", nullValue);
                        });
                    default:
                        throw new IllegalArgumentException();
                }
            }

            @Override
            public List<SyntheticSourceInvalidExample> invalidExample() throws IOException {
                return List.of(
                    new SyntheticSourceInvalidExample(
                        equalTo("field [field] of type [boolean] doesn't support synthetic source because it doesn't have doc values"),
                        b -> b.field("type", "boolean").field("doc_values", false)
                    )
                // If boolean had ignore_malformed we'd fail to index here
                );
            }
        };
    }

    protected IngestScriptSupport ingestScriptSupport() {
        return new IngestScriptSupport() {
            @Override
            protected BooleanFieldScript.Factory emptyFieldScript() {
                return (fieldName, params, searchLookup) -> ctx -> new BooleanFieldScript(fieldName, params, searchLookup, ctx) {
                    @Override
                    public void execute() {}
                };
            }

            @Override
            protected BooleanFieldScript.Factory nonEmptyFieldScript() {
                return (fieldName, params, searchLookup) -> ctx -> new BooleanFieldScript(fieldName, params, searchLookup, ctx) {
                    @Override
                    public void execute() {
                        emit(true);
                    }
                };
            }
        };
    }
}
