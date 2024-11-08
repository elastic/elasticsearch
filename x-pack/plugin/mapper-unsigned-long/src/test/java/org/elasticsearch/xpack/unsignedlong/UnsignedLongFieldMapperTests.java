/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.unsignedlong;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.NumberTypeOutOfRangeSpec;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.index.mapper.TimeSeriesRoutingHashFieldMapper;
import org.elasticsearch.index.mapper.WholeNumberFieldMapperTests;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class UnsignedLongFieldMapperTests extends WholeNumberFieldMapperTests {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new UnsignedLongMapperPlugin());
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "unsigned_long");
    }

    @Override
    protected Object getSampleValueForDocument() {
        return 123;
    }

    @Override
    protected boolean supportsSearchLookup() {
        return false;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("doc_values", b -> b.field("doc_values", false));
        checker.registerConflictCheck("index", b -> b.field("index", false));
        checker.registerConflictCheck("store", b -> b.field("store", true));
        checker.registerConflictCheck("null_value", b -> b.field("null_value", 1));
    }

    public void testDefaults() throws Exception {
        XContentBuilder mapping = fieldMapping(b -> b.field("type", "unsigned_long"));
        DocumentMapper mapper = createDocumentMapper(mapping);
        assertEquals(Strings.toString(mapping), mapper.mappingSource().toString());

        // test indexing of values as string
        {
            ParsedDocument doc = mapper.parse(source(b -> b.field("field", "18446744073709551615")));
            List<IndexableField> fields = doc.rootDoc().getFields("field");
            assertEquals(1, fields.size());
            assertEquals("LongField <field:9223372036854775807>", fields.get(0).toString());
        }

        // test indexing values as integer numbers
        {
            ParsedDocument doc = mapper.parse(source(b -> b.field("field", 9223372036854775807L)));
            List<IndexableField> fields = doc.rootDoc().getFields("field");
            assertEquals(1, fields.size());
            assertEquals("LongField <field:-1>", fields.get(0).toString());
        }

        // test that indexing values as number with decimal is not allowed
        {
            ThrowingRunnable runnable = () -> mapper.parse(source(b -> b.field("field", 10.5)));
            DocumentParsingException e = expectThrows(DocumentParsingException.class, runnable);
            assertThat(e.getCause().getMessage(), containsString("Value \"10.5\" has a decimal part"));
        }
    }

    public void testNotIndexed() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "unsigned_long").field("index", false)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "18446744073709551615")));
        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        IndexableField dvField = fields.get(0);
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertEquals(9223372036854775807L, dvField.numericValue().longValue());
    }

    public void testNoDocValues() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "unsigned_long").field("doc_values", false)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "18446744073709551615")));
        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        IndexableField pointField = fields.get(0);
        assertEquals(1, pointField.fieldType().pointIndexDimensionCount());
        assertEquals(9223372036854775807L, pointField.numericValue().longValue());
        assertAggregatableConsistency(mapper.mappers().getFieldType("field"));
    }

    public void testStore() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "unsigned_long").field("store", true)));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "18446744073709551615")));
        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.size());
        assertEquals("LongField <field:9223372036854775807>", fields.get(0).toString());
        IndexableField storedField = fields.get(1);
        assertTrue(storedField.fieldType().stored());
        assertEquals("18446744073709551615", storedField.stringValue());
    }

    public void testCoerceMappingParameterIsIllegal() {
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(fieldMapping(b -> b.field("type", "unsigned_long").field("coerce", false)))
        );
        assertThat(
            e.getMessage(),
            containsString("Failed to parse mapping: unknown parameter [coerce] on mapper [field] of type [unsigned_long]")
        );
    }

    public void testNullValue() throws IOException {
        // test that if null value is not defined, field is not indexed
        {
            DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
            ParsedDocument doc = mapper.parse(source(b -> b.nullField("field")));
            assertThat(doc.rootDoc().getFields("field"), empty());
        }

        // test that if null value is defined, it is used
        {
            DocumentMapper mapper = createDocumentMapper(
                fieldMapping(b -> b.field("type", "unsigned_long").field("null_value", "18446744073709551615"))
            );
            ParsedDocument doc = mapper.parse(source(b -> b.nullField("field")));
            ;
            List<IndexableField> fields = doc.rootDoc().getFields("field");
            assertEquals(1, fields.size());
            assertEquals("LongField <field:9223372036854775807>", fields.get(0).toString());
        }
    }

    @Override
    public void testCoerce() {} // coerce is unimplemented

    @Override
    protected boolean supportsIgnoreMalformed() {
        return true;
    }

    @Override
    protected List<ExampleMalformedValue> exampleMalformedValues() {
        return List.of(
            exampleMalformedValue("a").errorMatches("For input string: \"a\""),
            exampleMalformedValue(b -> b.value(false)).errorMatches("For input string: \"false\"")
        );
    }

    public void testDecimalParts() throws IOException {
        XContentBuilder mapping = fieldMapping(b -> b.field("type", "unsigned_long"));
        DocumentMapper mapper = createDocumentMapper(mapping);
        {
            ThrowingRunnable runnable = () -> mapper.parse(source(b -> b.field("field", randomFrom("100.5", 100.5, 100.5f))));
            DocumentParsingException e = expectThrows(DocumentParsingException.class, runnable);
            assertThat(e.getCause().getMessage(), containsString("Value \"100.5\" has a decimal part"));
        }
        {
            ThrowingRunnable runnable = () -> mapper.parse(source(b -> b.field("field", randomFrom("0.9", 0.9, 0.9f))));
            DocumentParsingException e = expectThrows(DocumentParsingException.class, runnable);
            assertThat(e.getCause().getMessage(), containsString("Value \"0.9\" has a decimal part"));
        }
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", randomFrom("100.", "100.0", "100.00", 100.0, 100.0f))));
        assertThat(doc.rootDoc().getFields("field").get(0).numericValue().longValue(), equalTo(Long.MIN_VALUE + 100L));

        doc = mapper.parse(source(b -> b.field("field", randomFrom("0.", "0.0", ".00", 0.0, 0.0f))));
        assertThat(doc.rootDoc().getFields("field").get(0).numericValue().longValue(), equalTo(Long.MIN_VALUE));
    }

    public void testIndexingOutOfRangeValues() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        for (Object outOfRangeValue : new Object[] { "-1", -1L, "18446744073709551616", new BigInteger("18446744073709551616") }) {
            ThrowingRunnable runnable = () -> mapper.parse(source(b -> b.field("field", outOfRangeValue)));
            expectThrows(DocumentParsingException.class, runnable);
        }
    }

    public void testExistsQueryDocValuesDisabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", false);
        }));
        assertExistsQuery(mapperService);
        assertParseMinimalWarnings();
    }

    public void testDimension() throws IOException {
        // Test default setting
        MapperService mapperService = createMapperService(fieldMapping(b -> minimalMapping(b)));
        UnsignedLongFieldMapper.UnsignedLongFieldType ft = (UnsignedLongFieldMapper.UnsignedLongFieldType) mapperService.fieldType("field");
        assertFalse(ft.isDimension());

        assertDimension(true, UnsignedLongFieldMapper.UnsignedLongFieldType::isDimension);
        assertDimension(false, UnsignedLongFieldMapper.UnsignedLongFieldType::isDimension);
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

    public void testDimensionMultiValuedFieldTSDB() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_dimension", true);
        }), IndexMode.TIME_SERIES);

        ParsedDocument doc = mapper.parse(source(null, b -> {
            b.array("field", randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
            b.field("@timestamp", Instant.now());
        }, TimeSeriesRoutingHashFieldMapper.encode(randomInt())));
        assertThat(doc.docs().get(0).getFields("field"), hasSize(greaterThan(1)));
    }

    public void testDimensionMultiValuedFieldNonTSDB() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_dimension", true);
        }), randomFrom(IndexMode.STANDARD, IndexMode.LOGSDB));

        ParsedDocument doc = mapper.parse(source(b -> {
            b.array("field", randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
            b.field("@timestamp", Instant.now());
        }));
        assertThat(doc.docs().get(0).getFields("field"), hasSize(greaterThan(1)));
    }

    public void testMetricType() throws IOException {
        // Test default setting
        MapperService mapperService = createMapperService(fieldMapping(b -> minimalMapping(b)));
        UnsignedLongFieldMapper.UnsignedLongFieldType ft = (UnsignedLongFieldMapper.UnsignedLongFieldType) mapperService.fieldType("field");
        assertNull(ft.getMetricType());

        assertMetricType("gauge", UnsignedLongFieldMapper.UnsignedLongFieldType::getMetricType);
        assertMetricType("counter", UnsignedLongFieldMapper.UnsignedLongFieldType::getMetricType);

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

    public void testMetricAndDocvalues() {
        Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_metric", "counter").field("doc_values", false);
        })));
        assertThat(e.getCause().getMessage(), containsString("Field [time_series_metric] requires that [doc_values] is true"));
    }

    public void testMetricAndDimension() {
        Exception e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_metric", "counter").field("time_series_dimension", true);
        })));
        assertThat(
            e.getCause().getMessage(),
            containsString("Field [time_series_dimension] cannot be set in conjunction with field [time_series_metric]")
        );
    }

    public void testTimeSeriesIndexDefault() throws Exception {
        var randomMetricType = randomFrom(TimeSeriesParams.MetricType.scalar());
        var indexSettings = getIndexSettingsBuilder().put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dimension_field");
        var mapperService = createMapperService(indexSettings.build(), fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_metric", randomMetricType.toString());
        }));
        var ft = (UnsignedLongFieldMapper.UnsignedLongFieldType) mapperService.fieldType("field");
        assertThat(ft.getMetricType(), equalTo(randomMetricType));
        assertThat(ft.isIndexed(), is(false));
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        Number n = randomNumericValue();
        return randomBoolean() ? n : n.toString();
    }

    private Number randomNumericValue() {
        switch (randomInt(8)) {
            case 0:
                return randomNonNegativeByte();
            case 1:
                return (short) between(0, Short.MAX_VALUE);
            case 2:
                return randomInt(Integer.MAX_VALUE);
            case 3:
            case 4:
                return randomNonNegativeLong();
            default:
                BigInteger big = BigInteger.valueOf(randomLongBetween(0, Long.MAX_VALUE)).shiftLeft(1);
                return big.add(randomBoolean() ? BigInteger.ONE : BigInteger.ZERO);
        }
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        return new NumberSyntheticSourceSupport(ignoreMalformed);
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }

    @Override
    protected List<NumberTypeOutOfRangeSpec> outOfRangeSpecs() {
        return Collections.emptyList(); // unimplemented
    }

    @Override
    public void testIgnoreMalformedWithObject() {} // unimplemented

    @Override
    public void testAllowMultipleValuesField() {} // unimplemented

    @Override
    public void testScriptableTypes() {} // unimplemented

    @Override
    protected Number missingValue() {
        return 123L;
    }

    @Override
    protected Number randomNumber() {
        if (randomBoolean()) {
            return randomLong();
        }
        if (randomBoolean()) {
            return randomDouble();
        }
        assumeFalse("https://github.com/elastic/elasticsearch/issues/70585", true);
        return randomDoubleBetween(0L, Long.MAX_VALUE, true);
    }

    protected Function<Object, Object> loadBlockExpected() {
        return v -> {
            // Numbers are in the block as a long but the test needs to compare them to their BigInteger value parsed from xcontent.
            if (v instanceof BigInteger ul) {
                if (ul.bitLength() < Long.SIZE) {
                    return ul.longValue() ^ Long.MIN_VALUE;
                }
                return ul.subtract(BigInteger.ONE.shiftLeft(Long.SIZE - 1)).longValue();
            }
            return ((Long) v).longValue() ^ Long.MIN_VALUE;
        };
    }

    final class NumberSyntheticSourceSupport implements SyntheticSourceSupport {
        private final BigInteger nullValue = usually() ? null : BigInteger.valueOf(randomNonNegativeLong());
        private final boolean ignoreMalformedEnabled;

        NumberSyntheticSourceSupport(boolean ignoreMalformedEnabled) {
            this.ignoreMalformedEnabled = ignoreMalformedEnabled;
        }

        @Override
        public SyntheticSourceExample example(int maxVals) {
            if (randomBoolean()) {
                Value v = generateValue();
                if (v.malformedOutput == null) {
                    return new SyntheticSourceExample(v.input, v.output, this::mapping);
                }
                return new SyntheticSourceExample(v.input, v.malformedOutput, null, this::mapping);
            }
            List<Value> values = randomList(1, maxVals, this::generateValue);
            List<Object> in = values.stream().map(Value::input).toList();

            List<BigInteger> outputFromDocValues = values.stream()
                .filter(v -> v.malformedOutput == null)
                .map(Value::output)
                .sorted()
                .toList();
            Stream<Object> malformedOutput = values.stream().filter(v -> v.malformedOutput != null).map(Value::malformedOutput);

            // Malformed values are always last in the implementation.
            List<Object> outList = Stream.concat(outputFromDocValues.stream(), malformedOutput).toList();
            Object out = outList.size() == 1 ? outList.get(0) : outList;

            Object outBlock = outputFromDocValues.size() == 1 ? outputFromDocValues.get(0) : outputFromDocValues;

            return new SyntheticSourceExample(in, out, outBlock, this::mapping);
        }

        private record Value(Object input, BigInteger output, Object malformedOutput) {}

        private Value generateValue() {
            if (nullValue != null && randomBoolean()) {
                return new Value(null, nullValue, null);
            }
            if (ignoreMalformedEnabled && randomBoolean()) {
                List<Supplier<Object>> choices = List.of(() -> randomAlphaOfLengthBetween(1, 10));
                var malformedInput = randomFrom(choices).get();
                return new Value(malformedInput, null, malformedInput);
            }
            long n = randomNonNegativeLong();
            BigInteger b = BigInteger.valueOf(n);
            if (b.signum() < 0) {
                b = b.add(BigInteger.ONE.shiftLeft(64));
            }
            return new Value(n, b, null);
        }

        private void mapping(XContentBuilder b) throws IOException {
            minimalMapping(b);
            if (nullValue != null) {
                b.field("null_value", nullValue);
            }
            if (rarely()) {
                b.field("index", false);
            }
            if (rarely()) {
                b.field("store", false);
            }
            if (ignoreMalformedEnabled) {
                b.field("ignore_malformed", "true");
            }
        }

        @Override
        public List<SyntheticSourceInvalidExample> invalidExample() {
            return List.of();
        }
    }
}
