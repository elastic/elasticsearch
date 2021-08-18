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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.NumberFieldTypeTests.OutOfRangeSpec;
import org.elasticsearch.index.termvectors.TermVectorsService;
import org.elasticsearch.script.DoubleFieldScript;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public abstract class NumberFieldMapperTests extends MapperTestCase {

    /**
     * @return a List of OutOfRangeSpec to test for this number type
     */
    protected abstract List<OutOfRangeSpec> outOfRangeSpecs();

    /**
     * @return an appropriate value to use for a missing value for this number type
     */
    protected abstract Number missingValue();

    /**
     * @return does this mapper allow index time scripts
     */
    protected boolean allowsIndexTimeScript() {
        return false;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("doc_values", b -> b.field("doc_values", false));
        checker.registerConflictCheck("index", b -> b.field("index", false));
        checker.registerConflictCheck("store", b -> b.field("store", true));
        checker.registerConflictCheck("null_value", b -> b.field("null_value", 1));
        checker.registerUpdateCheck(b -> b.field("coerce", false),
            m -> assertFalse(((NumberFieldMapper) m).coerce()));
        checker.registerUpdateCheck(b -> b.field("ignore_malformed", true),
            m -> assertTrue(((NumberFieldMapper) m).ignoreMalformed()));

        if (allowsIndexTimeScript()) {
            checker.registerConflictCheck("script", b -> b.field("script", "foo"));
            checker.registerUpdateCheck(
                b -> {
                    minimalMapping(b);
                    b.field("script", "test");
                    b.field("on_script_error", "fail");
                },
                b -> {
                    minimalMapping(b);
                    b.field("script", "test");
                    b.field("on_script_error", "continue");
                },
                m -> assertThat((m).onScriptError, equalTo("continue")));
        }
    }

    @Override
    protected Object getSampleValueForDocument() {
        return 123;
    }

    public void testExistsQueryDocValuesDisabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", false);
        }));
        assertExistsQuery(mapperService);
        assertParseMinimalWarnings();
    }

    public void testDefaults() throws Exception {
        XContentBuilder mapping = fieldMapping(this::minimalMapping);
        DocumentMapper mapper = createDocumentMapper(mapping);
        assertEquals(Strings.toString(mapping), mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", 123)));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertFalse(pointField.fieldType().stored());
        assertEquals(123, pointField.numericValue().doubleValue(), 0d);
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertFalse(dvField.fieldType().stored());
    }

    public void testNotIndexed() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("index", false);
        }));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", 123)));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField dvField = fields[0];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
    }

    public void testNoDocValues() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", false);
        }));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", 123)));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertEquals(123, pointField.numericValue().doubleValue(), 0d);
    }

    public void testStore() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("store", true);
        }));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", 123)));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(3, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertEquals(123, pointField.numericValue().doubleValue(), 0d);
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        IndexableField storedField = fields[2];
        assertTrue(storedField.fieldType().stored());
        assertEquals(123, storedField.numericValue().doubleValue(), 0d);
    }

    public void testCoerce() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "123")));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertEquals(123, pointField.numericValue().doubleValue(), 0d);
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());

        DocumentMapper mapper2 = createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("coerce", false);
        }));
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> mapper2.parse(source(b -> b.field("field", "123"))));
        assertThat(e.getCause().getMessage(), containsString("passed as String"));
    }

    public void testIgnoreMalformed() throws Exception {
        DocumentMapper notIgnoring = createDocumentMapper(fieldMapping(this::minimalMapping));
        DocumentMapper ignoring = createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("ignore_malformed", true);
        }));
        for (Object malformedValue : new Object[]{"a", Boolean.FALSE}) {
            SourceToParse source = source(b -> b.field("field", malformedValue));
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> notIgnoring.parse(source));
            if (malformedValue instanceof String) {
                assertThat(e.getCause().getMessage(), containsString("For input string: \"a\""));
            } else {
                assertThat(e.getCause().getMessage(), containsString("Current token"));
                assertThat(e.getCause().getMessage(), containsString("not numeric, can not use numeric value accessors"));
            }

            ParsedDocument doc = ignoring.parse(source);
            IndexableField[] fields = doc.rootDoc().getFields("field");
            assertEquals(0, fields.length);
            assertArrayEquals(new String[]{"field"}, TermVectorsService.getValues(doc.rootDoc().getFields("_ignored")));
        }
    }

    /**
     * Test that in case the malformed value is an xContent object we throw error regardless of `ignore_malformed`
     */
    public void testIgnoreMalformedWithObject() throws Exception {
        SourceToParse malformed = source(b -> b.startObject("field").field("foo", "bar").endObject());
        for (Boolean ignoreMalformed : new Boolean[]{true, false}) {
            DocumentMapper mapper = createDocumentMapper(
                fieldMapping(b -> {
                    minimalMapping(b);
                    b.field("ignore_malformed", ignoreMalformed);
                })
            );
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> mapper.parse(malformed));
            assertThat(e.getCause().getMessage(), containsString("Current token"));
            assertThat(e.getCause().getMessage(), containsString("not numeric, can not use numeric value accessors"));
        }
    }

    protected void testNullValue() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        SourceToParse source = source(b -> b.nullField("field"));
        ParsedDocument doc = mapper.parse(source);
        assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field"));

        Number missing = missingValue();
        mapper = createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("null_value", missing);
        }));
        doc = mapper.parse(source);
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertFalse(pointField.fieldType().stored());
        assertEquals(123, pointField.numericValue().doubleValue(), 0d);
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertFalse(dvField.fieldType().stored());
    }

    public void testOutOfRangeValues() throws IOException {
        for(OutOfRangeSpec item : outOfRangeSpecs()) {
            DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", item.type.typeName())));
            Exception e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(item::write)));
            assertThat("Incorrect error message for [" + item.type + "] with value [" + item.value + "]",
                e.getCause().getMessage(), containsString(item.message));
        }
    }

    public void testDimension() throws IOException {
        // Test default setting
        MapperService mapperService = createMapperService(fieldMapping(b -> minimalMapping(b)));
        NumberFieldMapper.NumberFieldType ft = (NumberFieldMapper.NumberFieldType) mapperService.fieldType("field");
        assertFalse(ft.isDimension());

        // dimension = false is allowed
        assertDimension(false, NumberFieldMapper.NumberFieldType::isDimension);

        // dimension = true is not allowed
        Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("dimension", true);
        })));
        assertThat(e.getCause().getMessage(), containsString("Parameter [dimension] cannot be set"));
    }

    protected <T> void assertMetricType(String metricType, Function<T, Enum> checker)
        throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("metric", metricType);
        }));

        @SuppressWarnings("unchecked") // Syntactic sugar in tests
        T fieldType = (T) mapperService.fieldType("field");
        assertThat(checker.apply(fieldType).name(), equalTo(metricType));
    }

    public void testMetricType() throws IOException {
        // Test default setting
        MapperService mapperService = createMapperService(fieldMapping(b -> minimalMapping(b)));
        NumberFieldMapper.NumberFieldType ft = (NumberFieldMapper.NumberFieldType) mapperService.fieldType("field");
        assertNull(ft.getMetricType());

        assertMetricType("gauge", NumberFieldMapper.NumberFieldType::getMetricType);
        assertMetricType("counter", NumberFieldMapper.NumberFieldType::getMetricType);

        // Test invalid metric type for this field type
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("metric", "histogram");
        })));
        assertThat(
            e.getCause().getMessage(),
            containsString("Unknown value [histogram] for field [metric] - accepted values are [null, gauge, counter]")
        );
    }

    public void testMetricAndDocvalues() {
        Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("metric", "counter").field("doc_values", false);
        })));
        assertThat(e.getCause().getMessage(), containsString("Field [metric] requires that [doc_values] is true"));
    }

    @Override
    protected final Object generateRandomInputValue(MappedFieldType ft) {
        Number n = randomNumber();
        return randomBoolean() ? n : n.toString();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T> T compileScript(Script script, ScriptContext<T> context) {
        if (context == LongFieldScript.CONTEXT) {
            return (T) LongFieldScript.PARSE_FROM_SOURCE;
        }
        if (context == DoubleFieldScript.CONTEXT) {
            return (T) DoubleFieldScript.PARSE_FROM_SOURCE;
        }
        throw new UnsupportedOperationException("Unknown script " + script.getIdOrCode());
    }

    public void testScriptableTypes() throws IOException {
        if (allowsIndexTimeScript()) {
            createDocumentMapper(fieldMapping(b -> {
                minimalMapping(b);
                b.field("script", "foo");
            }));
        } else {
            Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
                minimalMapping(b);
                b.field("script", "foo");
            })));
            assertEquals("Failed to parse mapping: Unknown parameter [script] for mapper [field]", e.getMessage());
        }
    }

    protected abstract Number randomNumber();

}
