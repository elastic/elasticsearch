/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.script.DoubleFieldScript;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public abstract class NumberFieldMapperTests extends MapperTestCase {

    /**
     * @return a List of OutOfRangeSpec to test for this number type
     */
    protected abstract List<NumberTypeOutOfRangeSpec> outOfRangeSpecs();

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
        checker.registerUpdateCheck(b -> b.field("coerce", false), m -> assertFalse(((NumberFieldMapper) m).coerce()));

        if (allowsIndexTimeScript()) {
            checker.registerConflictCheck("script", b -> b.field("script", "foo"));
            checker.registerUpdateCheck(b -> {
                minimalMapping(b);
                b.field("script", "test");
                b.field("on_script_error", "fail");
            }, b -> {
                minimalMapping(b);
                b.field("script", "test");
                b.field("on_script_error", "continue");
            }, m -> assertThat((m).builderParams.onScriptError(), is(OnScriptError.CONTINUE)));
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

    public void testAggregationsDocValuesDisabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", false);
        }));
        assertAggregatableConsistency(mapperService.fieldType("field"));
    }

    public void testDefaults() throws Exception {
        XContentBuilder mapping = fieldMapping(this::minimalMapping);
        DocumentMapper mapper = createDocumentMapper(mapping);
        assertEquals(Strings.toString(mapping), mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", 123)));

        List<IndexableField> fields = doc.rootDoc().getFields("field");
        // One field indexes points
        assertEquals(1, fields.stream().filter(f -> f.fieldType().pointIndexDimensionCount() != 0).count());
        // One field indexes doc values
        assertEquals(1, fields.stream().filter(f -> f.fieldType().docValuesType() != DocValuesType.NONE).count());
    }

    public void testNotIndexed() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("index", false);
        }));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", 123)));

        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        IndexableField dvField = fields.get(0);
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
    }

    public void testNoDocValues() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", false);
        }));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", 123)));

        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        IndexableField pointField = fields.get(0);
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertEquals(123, pointField.numericValue().doubleValue(), 0d);
    }

    public void testStore() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("store", true);
        }));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", 123)));

        List<IndexableField> fields = doc.rootDoc().getFields("field");

        // One field indexes points
        assertEquals(1, fields.stream().filter(f -> f.fieldType().pointIndexDimensionCount() != 0).count());
        // One field indexes doc values
        assertEquals(1, fields.stream().filter(f -> f.fieldType().docValuesType() != DocValuesType.NONE).count());
        // One field is stored
        assertEquals(1, fields.stream().filter(f -> f.fieldType().stored()).count());
    }

    public void testCoerce() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "123")));

        List<IndexableField> fields = doc.rootDoc().getFields("field");
        // One field indexes points
        assertEquals(1, fields.stream().filter(f -> f.fieldType().pointIndexDimensionCount() != 0).count());
        // One field indexes doc values
        assertEquals(1, fields.stream().filter(f -> f.fieldType().docValuesType() != DocValuesType.NONE).count());

        DocumentMapper mapper2 = createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("coerce", false);
        }));
        Exception e = expectThrows(DocumentParsingException.class, () -> mapper2.parse(source(b -> b.field("field", "123"))));
        assertThat(e.getCause().getMessage(), containsString("passed as String"));
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return true;
    }

    @Override
    protected List<ExampleMalformedValue> exampleMalformedValues() {
        return List.of(
            exampleMalformedValue("a").errorMatches("For input string: \"a\""),
            exampleMalformedValue(b -> b.value(false)).errorMatches(
                both(containsString("Current token")).and(containsString("not numeric, can not use numeric value accessors"))
            )
        );
    }

    /**
     * Test that in case the malformed value is an xContent object we throw error regardless of `ignore_malformed`
     */
    public void testIgnoreMalformedWithObject() throws Exception {
        SourceToParse malformed = source(b -> b.startObject("field").field("foo", "bar").endObject());
        for (Boolean ignoreMalformed : new Boolean[] { true, false }) {
            DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
                minimalMapping(b);
                b.field("ignore_malformed", ignoreMalformed);
            }));
            DocumentParsingException e = expectThrows(DocumentParsingException.class, () -> mapper.parse(malformed));
            assertThat(e.getCause().getMessage(), containsString("Cannot parse object as number"));
        }
    }

    public void testNullValue() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        SourceToParse source = source(b -> b.nullField("field"));
        ParsedDocument doc = mapper.parse(source);
        assertThat(doc.rootDoc().getFields("field"), empty());

        Number missing = missingValue();
        mapper = createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("null_value", missing);
        }));
        doc = mapper.parse(source);
        List<IndexableField> fields = doc.rootDoc().getFields("field");
        List<IndexableField> pointFields = fields.stream().filter(f -> f.fieldType().pointIndexDimensionCount() != 0).toList();
        assertEquals(1, pointFields.size());
        assertEquals(1, pointFields.get(0).fieldType().pointIndexDimensionCount());
        assertFalse(pointFields.get(0).fieldType().stored());

        List<IndexableField> dvFields = fields.stream().filter(f -> f.fieldType().docValuesType() != DocValuesType.NONE).toList();
        assertEquals(1, dvFields.size());
        assertEquals(DocValuesType.SORTED_NUMERIC, dvFields.get(0).fieldType().docValuesType());
        assertFalse(dvFields.get(0).fieldType().stored());
    }

    public void testOutOfRangeValues() throws IOException {
        for (NumberTypeOutOfRangeSpec item : outOfRangeSpecs()) {
            DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", item.type.typeName())));
            Exception e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(item::write)));
            assertThat(
                "Incorrect error message for [" + item.type + "] with value [" + item.value + "]",
                e.getCause().getMessage(),
                containsString(item.message)
            );
        }
    }

    public void testDimension() throws IOException {
        // Test default setting
        MapperService mapperService = createMapperService(fieldMapping(b -> minimalMapping(b)));
        NumberFieldMapper.NumberFieldType ft = (NumberFieldMapper.NumberFieldType) mapperService.fieldType("field");
        assertFalse(ft.isDimension());

        // dimension = false is allowed
        assertDimension(false, NumberFieldMapper.NumberFieldType::isDimension);

        // dimension = true is allowed
        assertDimension(true, NumberFieldMapper.NumberFieldType::isDimension);
    }

    public void testMetricType() throws IOException {
        // Test default setting
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        NumberFieldMapper.NumberFieldType ft = (NumberFieldMapper.NumberFieldType) mapperService.fieldType("field");
        assertNull(ft.getMetricType());

        assertMetricType("gauge", NumberFieldMapper.NumberFieldType::getMetricType);
        assertMetricType("counter", NumberFieldMapper.NumberFieldType::getMetricType);

        {
            // Test invalid metric type for this field type
            Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
                minimalMapping(b);
                b.field("time_series_metric", "histogram");
            })));
            assertThat(
                e.getCause().getMessage(),
                containsString("Unknown value [histogram] for field [time_series_metric] - accepted values are [gauge, counter]")
            );
        }
        {
            // Test invalid metric type for this field type
            Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
                minimalMapping(b);
                b.field("time_series_metric", "unknown");
            })));
            assertThat(
                e.getCause().getMessage(),
                containsString("Unknown value [unknown] for field [time_series_metric] - accepted values are [gauge, counter]")
            );
        }
    }

    public void testTimeSeriesIndexDefault() throws Exception {
        var randomMetricType = randomFrom(TimeSeriesParams.MetricType.scalar());
        var indexSettings = getIndexSettingsBuilder().put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dimension_field");
        var mapperService = createMapperService(indexSettings.build(), fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_metric", randomMetricType.toString());
        }));
        var ft = (NumberFieldMapper.NumberFieldType) mapperService.fieldType("field");
        assertThat(ft.getMetricType(), equalTo(randomMetricType));
        assertThat(ft.isIndexed(), is(false));
    }

    public void testMetricAndDocvalues() {
        Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_metric", "counter").field("doc_values", false);
        })));
        assertThat(e.getCause().getMessage(), containsString("Field [time_series_metric] requires that [doc_values] is true"));
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        Number n = randomNumber();
        return randomBoolean() ? n : n.toString();
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        return new IngestScriptSupport() {
            @Override
            @SuppressWarnings("unchecked")
            protected <T> T compileOtherScript(Script script, ScriptContext<T> context) {
                if (context == LongFieldScript.CONTEXT) {
                    return (T) LongFieldScript.PARSE_FROM_SOURCE;
                }
                if (context == DoubleFieldScript.CONTEXT) {
                    return (T) DoubleFieldScript.PARSE_FROM_SOURCE;
                }
                throw new UnsupportedOperationException("Unknown script " + script.getIdOrCode());
            }

            @Override
            ScriptFactory emptyFieldScript() {
                return null;
            }

            @Override
            ScriptFactory nonEmptyFieldScript() {
                return null;
            }
        };
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

    public void testAllowMultipleValuesField() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> minimalMapping(b)));

        Mapper mapper = mapperService.mappingLookup().getMapper("field");
        if (mapper instanceof NumberFieldMapper numberFieldMapper) {
            numberFieldMapper.setAllowMultipleValues(false);
        } else {
            fail("mapper [" + mapper.getClass() + "] error, not number field");
        }

        Exception e = expectThrows(
            DocumentParsingException.class,
            () -> mapperService.documentMapper().parse(source(b -> b.array("field", randomNumber(), randomNumber(), randomNumber())))
        );
        assertThat(e.getCause().getMessage(), containsString("Only one field can be stored per key"));
    }

    protected abstract Number randomNumber();

    protected final class NumberSyntheticSourceSupportForKeepTests extends NumberSyntheticSourceSupport {
        private final boolean preserveSource;

        protected NumberSyntheticSourceSupportForKeepTests(
            Function<Number, Number> round,
            boolean ignoreMalformed,
            Mapper.SourceKeepMode sourceKeepMode
        ) {
            super(round, ignoreMalformed);
            this.preserveSource = sourceKeepMode == Mapper.SourceKeepMode.ALL;
        }

        @Override
        public boolean preservesExactSource() {
            return preserveSource;
        }

        @Override
        public SyntheticSourceExample example(int maxVals) {
            var example = super.example(maxVals);
            return new SyntheticSourceExample(
                example.expectedForSyntheticSource(),
                example.expectedForSyntheticSource(),
                example.mapping()
            );
        }
    }

    protected class NumberSyntheticSourceSupport implements SyntheticSourceSupport {
        private final Long nullValue = usually() ? null : randomNumber().longValue();
        private final boolean coerce = rarely();
        private final boolean docValues = randomBoolean();

        private final Function<Number, Number> round;
        private final boolean ignoreMalformed;

        protected NumberSyntheticSourceSupport(Function<Number, Number> round, boolean ignoreMalformed) {
            this.round = round;
            this.ignoreMalformed = ignoreMalformed;
        }

        @Override
        public boolean preservesExactSource() {
            // We opt in into fallback synthetic source if there is no doc values
            // which preserves exact source.
            return docValues == false;
        }

        @Override
        public SyntheticSourceExample example(int maxVals) {
            if (randomBoolean()) {
                Tuple<Object, Object> v = generateValue();
                if (preservesExactSource()) {
                    var rawInput = v.v1();
                    return new SyntheticSourceExample(rawInput, rawInput, this::mapping);
                }
                if (v.v2() instanceof Number n) {
                    Number result = round.apply(n);
                    return new SyntheticSourceExample(v.v1(), result, this::mapping);
                }
                // ignore_malformed value
                return new SyntheticSourceExample(v.v1(), v.v2(), this::mapping);
            }
            List<Tuple<Object, Object>> values = randomList(1, maxVals, this::generateValue);
            List<Object> in = values.stream().map(Tuple::v1).toList();

            if (preservesExactSource()) {
                return new SyntheticSourceExample(in, in, this::mapping);
            } else {
                List<Object> outList = values.stream()
                    .filter(v -> v.v2() instanceof Number)
                    .map(t -> round.apply((Number) t.v2()))
                    .sorted()
                    .collect(Collectors.toCollection(ArrayList::new));
                values.stream().filter(v -> false == v.v2() instanceof Number).map(Tuple::v2).forEach(outList::add);
                var out = outList.size() == 1 ? outList.get(0) : outList;

                return new SyntheticSourceExample(in, out, this::mapping);
            }
        }

        private Tuple<Object, Object> generateValue() {
            if (ignoreMalformed && randomBoolean()) {
                List<Supplier<Object>> choices = List.of(() -> "a" + randomAlphaOfLength(3), ESTestCase::randomBoolean);
                Object v = randomFrom(choices).get();
                return Tuple.tuple(v, v);
            }
            if (nullValue != null && randomBoolean()) {
                return Tuple.tuple(null, nullValue);
            }
            Number n = randomNumber();
            Object in = n;
            Number out = n;
            if (coerce && randomBoolean()) {
                in = in.toString();
            }
            return Tuple.tuple(in, out);
        }

        private void mapping(XContentBuilder b) throws IOException {
            minimalMapping(b);
            if (coerce) {
                b.field("coerce", true);
            }
            if (nullValue != null) {
                b.field("null_value", nullValue);
            }
            if (ignoreMalformed) {
                b.field("ignore_malformed", true);
            }
            if (docValues == false) {
                b.field("doc_values", "false");
            }
        }

        @Override
        public List<SyntheticSourceInvalidExample> invalidExample() throws IOException {
            return List.of();
        }
    }
}
