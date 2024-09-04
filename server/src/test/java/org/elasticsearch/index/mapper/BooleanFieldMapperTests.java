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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.script.BooleanFieldScript;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;
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

        registerDimensionChecks(checker);
    }

    public void testExistsQueryDocValuesDisabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", false);
        }));
        assertExistsQuery(mapperService);
        assertParseMinimalWarnings();
    }

    public void testAggregationsDocValuesDisabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", false);
        }));
        assertAggregatableConsistency(mapperService.fieldType("field"));
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
            DocumentParsingException ex = expectThrows(
                DocumentParsingException.class,
                () -> defaultMapper.parse(source(b -> b.field("field", value)))
            );
            assertEquals(
                "[1:10] failed to parse field [field] of type [boolean] in document with id '1'. "
                    + "Preview of field's value: '"
                    + value
                    + "'",
                ex.getMessage()
            );
        }
    }

    public void testParsesBooleansNestedStrict() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        DocumentParsingException ex = expectThrows(DocumentParsingException.class, () -> defaultMapper.parse(source(b -> {
            b.startObject("field");
            {
                b.field("inner_field", "no");
            }
            b.endObject();
        })));
        assertEquals(
            "[1:29] failed to parse field [field] of type [boolean] in document with id '1'. "
                + "Preview of field's value: '{inner_field=no}'",
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
        List<IndexableField> fields = doc.getFields("bool1");
        assertEquals(2, fields.size());
        assertEquals(DocValuesType.NONE, fields.get(0).fieldType().docValuesType());
        assertEquals(DocValuesType.SORTED_NUMERIC, fields.get(1).fieldType().docValuesType());
        fields = doc.getFields("bool2");
        assertEquals(1, fields.size());
        assertEquals(DocValuesType.SORTED_NUMERIC, fields.get(0).fieldType().docValuesType());
        fields = doc.getFields("bool3");
        assertEquals(DocValuesType.NONE, fields.get(0).fieldType().docValuesType());
        assertEquals(DocValuesType.SORTED_NUMERIC, fields.get(1).fieldType().docValuesType());
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

    public void testDimension() throws IOException {
        // Test default setting
        MapperService mapperService = createMapperService(fieldMapping(b -> minimalMapping(b)));
        BooleanFieldMapper.BooleanFieldType ft = (BooleanFieldMapper.BooleanFieldType) mapperService.fieldType("field");
        assertFalse(ft.isDimension());

        assertDimension(true, BooleanFieldMapper.BooleanFieldType::isDimension);
        assertDimension(false, BooleanFieldMapper.BooleanFieldType::isDimension);
    }

    public void testDimensionIndexedAndDocvalues() {
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                minimalMapping(b);
                b.field("time_series_dimension", true).field("index", false).field("doc_values", false);
            })));
            assertThat(
                e.getCause().getMessage(),
                containsString("Field [time_series_dimension] requires that [index] and [doc_values] are true")
            );
        }
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                minimalMapping(b);
                b.field("time_series_dimension", true).field("index", true).field("doc_values", false);
            })));
            assertThat(
                e.getCause().getMessage(),
                containsString("Field [time_series_dimension] requires that [index] and [doc_values] are true")
            );
        }
        {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                minimalMapping(b);
                b.field("time_series_dimension", true).field("index", false).field("doc_values", true);
            })));
            assertThat(
                e.getCause().getMessage(),
                containsString("Field [time_series_dimension] requires that [index] and [doc_values] are true")
            );
        }
    }

    public void testDimensionMultiValuedField() throws IOException {
        XContentBuilder mapping = fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_dimension", true);
        });
        DocumentMapper mapper = randomBoolean() ? createDocumentMapper(mapping) : createTimeSeriesModeDocumentMapper(mapping);

        Exception e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> b.array("field", true, false))));
        assertThat(e.getCause().getMessage(), containsString("Dimension field [field] cannot be a multi-valued field"));
    }

    public void testDimensionInRoutingPath() throws IOException {
        MapperService mapper = createMapperService(fieldMapping(b -> b.field("type", "keyword").field("time_series_dimension", true)));
        IndexSettings settings = createIndexSettings(
            IndexVersion.current(),
            Settings.builder()
                .put(IndexSettings.MODE.getKey(), "time_series")
                .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "field")
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T00:00:00Z")
                .build()
        );
        mapper.documentMapper().validate(settings, false);  // Doesn't throw
    }

    @Override
    protected List<ExampleMalformedValue> exampleMalformedValues() {
        return List.of(exampleMalformedValue("a").errorMatches("Failed to parse value [a] as only [true] or [false] are allowed."));
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return true;
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        return new SyntheticSourceSupport() {
            Boolean nullValue = usually() ? null : randomBoolean();

            @Override
            public SyntheticSourceExample example(int maxVals) throws IOException {
                if (randomBoolean()) {
                    Tuple<Boolean, Boolean> v = generateValue();
                    return new SyntheticSourceExample(v.v1(), v.v2(), this::mapping);
                }
                List<Tuple<Boolean, Boolean>> values = randomList(1, maxVals, this::generateValue);
                List<Boolean> in = values.stream().map(Tuple::v1).toList();
                List<Boolean> outList = values.stream().map(Tuple::v2).sorted().toList();
                Object out = outList.size() == 1 ? outList.get(0) : outList;
                return new SyntheticSourceExample(in, out, this::mapping);
            }

            private Tuple<Boolean, Boolean> generateValue() {
                if (nullValue != null && randomBoolean()) {
                    return Tuple.tuple(null, nullValue);
                }
                boolean b = randomBoolean();
                return Tuple.tuple(b, b);
            }

            private void mapping(XContentBuilder b) throws IOException {
                minimalMapping(b);
                if (nullValue != null) {
                    b.field("null_value", nullValue);
                }
                b.field("ignore_malformed", ignoreMalformed);
            }

            @Override
            public List<SyntheticSourceInvalidExample> invalidExample() throws IOException {
                return List.of(
                    new SyntheticSourceInvalidExample(
                        equalTo("field [field] of type [boolean] doesn't support synthetic source because it doesn't have doc values"),
                        b -> b.field("type", "boolean").field("doc_values", false)
                    )
                );
            }
        };
    }

    @Override
    protected Function<Object, Object> loadBlockExpected() {
        // Just assert that we expect a boolean. Otherwise no munging.
        return v -> (Boolean) v;
    }

    protected IngestScriptSupport ingestScriptSupport() {
        return new IngestScriptSupport() {
            @Override
            protected BooleanFieldScript.Factory emptyFieldScript() {
                return (fieldName, params, searchLookup, onScriptError) -> ctx -> new BooleanFieldScript(
                    fieldName,
                    params,
                    searchLookup,
                    OnScriptError.FAIL,
                    ctx
                ) {
                    @Override
                    public void execute() {}
                };
            }

            @Override
            protected BooleanFieldScript.Factory nonEmptyFieldScript() {
                return (fieldName, params, searchLookup, onScriptError) -> ctx -> new BooleanFieldScript(
                    fieldName,
                    params,
                    searchLookup,
                    OnScriptError.FAIL,
                    ctx
                ) {
                    @Override
                    public void execute() {
                        emit(true);
                    }
                };
            }
        };
    }
}
