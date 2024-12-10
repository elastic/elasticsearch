/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.NumberFieldMapperTests;
import org.elasticsearch.index.mapper.NumberTypeOutOfRangeSpec;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matcher;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notANumber;

public class ScaledFloatFieldMapperTests extends NumberFieldMapperTests {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return singletonList(new MapperExtrasPlugin());
    }

    @Override
    protected Object getSampleValueForDocument() {
        return 123;
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "scaled_float").field("scaling_factor", 10.0);
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("scaling_factor", fieldMapping(this::minimalMapping), fieldMapping(b -> {
            b.field("type", "scaled_float");
            b.field("scaling_factor", 5.0);
        }));
        checker.registerConflictCheck("doc_values", b -> b.field("doc_values", false));
        checker.registerConflictCheck("index", b -> b.field("index", false));
        checker.registerConflictCheck("store", b -> b.field("store", true));
        checker.registerConflictCheck("null_value", b -> b.field("null_value", 1));
        checker.registerUpdateCheck(b -> b.field("coerce", false), m -> assertFalse(((ScaledFloatFieldMapper) m).coerce()));
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
        XContentBuilder mapping = fieldMapping(b -> b.field("type", "scaled_float").field("scaling_factor", 10.0));
        DocumentMapper mapper = createDocumentMapper(mapping);
        assertEquals(Strings.toString(mapping), mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", 123)));
        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        assertEquals("LongField <field:1230>", fields.get(0).toString());
    }

    public void testMissingScalingFactor() {
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(fieldMapping(b -> b.field("type", "scaled_float")))
        );
        assertThat(e.getMessage(), containsString("Failed to parse mapping: Field [scaling_factor] is required"));
    }

    public void testIllegalScalingFactor() {
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(fieldMapping(b -> b.field("type", "scaled_float").field("scaling_factor", -1)))
        );
        assertThat(e.getMessage(), containsString("[scaling_factor] must be a positive number, got [-1.0]"));
    }

    public void testNotIndexed() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "scaled_float").field("index", false).field("scaling_factor", 10.0))
        );

        ParsedDocument doc = mapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", 123).endObject()),
                XContentType.JSON
            )
        );

        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        IndexableField dvField = fields.get(0);
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertEquals(1230, dvField.numericValue().longValue());
    }

    public void testNoDocValues() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "scaled_float").field("doc_values", false).field("scaling_factor", 10.0))
        );

        ParsedDocument doc = mapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", 123).endObject()),
                XContentType.JSON
            )
        );

        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        IndexableField pointField = fields.get(0);
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        assertEquals(1230, pointField.numericValue().longValue());
    }

    public void testDocValuesSearchable() throws Exception {
        boolean[] indexables = new boolean[] { true, false };
        boolean[] hasDocValues = new boolean[] { true, false };
        for (boolean indexable : indexables) {
            for (boolean hasDocValue : hasDocValues) {
                MapperService mapperService = createMapperService(
                    fieldMapping(
                        b -> b.field("type", "scaled_float")
                            .field("index", indexable)
                            .field("doc_values", hasDocValue)
                            .field("scaling_factor", 10.0)
                    )
                );
                MappedFieldType fieldType = mapperService.fieldType("field");
                assertEquals(fieldType.isSearchable(), indexable || hasDocValue);
            }
        }
    }

    public void testStore() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "scaled_float").field("store", true).field("scaling_factor", 10.0))
        );

        ParsedDocument doc = mapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", 123).endObject()),
                XContentType.JSON
            )
        );

        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.size());
        assertEquals("LongField <field:1230>", fields.get(0).toString());
        IndexableField storedField = fields.get(1);
        assertTrue(storedField.fieldType().stored());
        assertEquals(1230, storedField.numericValue().longValue());
    }

    public void testCoerce() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", "123").endObject()),
                XContentType.JSON
            )
        );
        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        assertEquals("LongField <field:1230>", fields.get(0).toString());

        DocumentMapper mapper2 = createDocumentMapper(
            fieldMapping(b -> b.field("type", "scaled_float").field("scaling_factor", 10.0).field("coerce", false))
        );
        ThrowingRunnable runnable = () -> mapper2.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", "123").endObject()),
                XContentType.JSON
            )
        );
        DocumentParsingException e = expectThrows(DocumentParsingException.class, runnable);
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
            exampleMalformedValue("NaN").errorMatches("[scaled_float] only supports finite values, but got [NaN]"),
            exampleMalformedValue("Infinity").errorMatches("[scaled_float] only supports finite values, but got [Infinity]"),
            exampleMalformedValue("-Infinity").errorMatches("[scaled_float] only supports finite values, but got [-Infinity]"),
            exampleMalformedValue(b -> b.value(true)).errorMatches("Current token (VALUE_TRUE) not numeric")
        );
    }

    public void testNullValue() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject().nullField("field").endObject()),
                XContentType.JSON
            )
        );
        assertThat(doc.rootDoc().getFields("field"), empty());

        mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "scaled_float").field("scaling_factor", 10.0).field("null_value", 2.5))
        );
        doc = mapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject().nullField("field").endObject()),
                XContentType.JSON
            )
        );
        List<IndexableField> fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.size());
        assertEquals("LongField <field:25>", fields.get(0).toString());
    }

    /**
     * `index_options` was deprecated and is rejected as of 7.0
     */
    public void testRejectIndexOptions() {
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(fieldMapping(b -> b.field("type", "scaled_float").field("index_options", randomIndexOptions())))
        );
        assertThat(
            e.getMessage(),
            containsString("Failed to parse mapping: unknown parameter [index_options] on mapper [field] of type [scaled_float]")
        );
    }

    public void testMetricType() throws IOException {
        // Test default setting
        MapperService mapperService = createMapperService(fieldMapping(b -> minimalMapping(b)));
        ScaledFloatFieldMapper.ScaledFloatFieldType ft = (ScaledFloatFieldMapper.ScaledFloatFieldType) mapperService.fieldType("field");
        assertNull(ft.getMetricType());

        assertMetricType("gauge", ScaledFloatFieldMapper.ScaledFloatFieldType::getMetricType);
        assertMetricType("counter", ScaledFloatFieldMapper.ScaledFloatFieldType::getMetricType);

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

    public void testTimeSeriesIndexDefault() throws Exception {
        var randomMetricType = randomFrom(TimeSeriesParams.MetricType.scalar());
        var indexSettings = getIndexSettingsBuilder().put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dimension_field");
        var mapperService = createMapperService(indexSettings.build(), fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_metric", randomMetricType.toString());
        }));
        var ft = (ScaledFloatFieldMapper.ScaledFloatFieldType) mapperService.fieldType("field");
        assertThat(ft.getMetricType(), equalTo(randomMetricType));
        assertThat(ft.isIndexed(), is(false));
    }

    @Override
    protected void randomFetchTestFieldConfig(XContentBuilder b) throws IOException {
        // Large floats are a terrible idea but the round trip should still work no matter how badly you configure the field
        b.field("type", "scaled_float").field("scaling_factor", randomDoubleBetween(0, Float.MAX_VALUE, true));
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        /*
         * randomDoubleBetween will smear the random values out across a huge
         * range of valid values.
         */
        double v = randomDoubleBetween(-Float.MAX_VALUE, Float.MAX_VALUE, true);
        return switch (between(0, 3)) {
            case 0 -> v;
            case 1 -> (float) v;
            case 2 -> Double.toString(v);
            case 3 -> Float.toString((float) v);
            default -> throw new IllegalArgumentException();
        };
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        return new ScaledFloatSyntheticSourceSupport(ignoreMalformed);
    }

    private static class ScaledFloatSyntheticSourceSupport implements SyntheticSourceSupport {
        private final boolean ignoreMalformedEnabled;
        private final double scalingFactor = randomDoubleBetween(0, Double.MAX_VALUE, false);
        private final Double nullValue = usually() ? null : round(randomValue());

        private ScaledFloatSyntheticSourceSupport(boolean ignoreMalformedEnabled) {
            this.ignoreMalformedEnabled = ignoreMalformedEnabled;
        }

        @Override
        public SyntheticSourceExample example(int maxValues) {
            if (randomBoolean()) {
                Value v = generateValue();
                if (v.malformedOutput == null) {
                    return new SyntheticSourceExample(v.input, v.output, roundDocValues(v.output), this::mapping);
                }
                return new SyntheticSourceExample(v.input, v.malformedOutput, null, this::mapping);
            }
            List<Value> values = randomList(1, maxValues, this::generateValue);
            List<Object> in = values.stream().map(Value::input).toList();

            List<Double> outputFromDocValues = values.stream().filter(v -> v.malformedOutput == null).map(Value::output).sorted().toList();
            Stream<Object> malformedOutput = values.stream().filter(v -> v.malformedOutput != null).map(Value::malformedOutput);

            // Malformed values are always last in the implementation.
            List<Object> outList = Stream.concat(outputFromDocValues.stream(), malformedOutput).toList();
            Object out = outList.size() == 1 ? outList.get(0) : outList;

            List<Double> outBlockList = outputFromDocValues.stream().map(this::roundDocValues).sorted().toList();
            Object outBlock = outBlockList.size() == 1 ? outBlockList.get(0) : outBlockList;
            return new SyntheticSourceExample(in, out, outBlock, this::mapping);
        }

        private record Value(Object input, Double output, Object malformedOutput) {}

        private Value generateValue() {
            if (nullValue != null && randomBoolean()) {
                return new Value(null, nullValue, null);
            }
            // Examples in #exampleMalformedValues() are also tested with synthetic source
            // so this is not an exhaustive list.
            // Here we mostly want to verify behavior of arrays that contain malformed
            // values since there are modifications specific to synthetic source.
            if (ignoreMalformedEnabled && randomBoolean()) {
                List<Supplier<Object>> choices = List.of(
                    () -> randomAlphaOfLengthBetween(1, 10),
                    () -> Map.of(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10))
                );
                var malformedInput = randomFrom(choices).get();
                return new Value(malformedInput, null, malformedInput);
            }
            double d = randomValue();
            return new Value(d, round(d), null);
        }

        private double round(double d) {
            long encoded = Math.round(d * scalingFactor);
            double decoded = encoded / scalingFactor;
            // Special case due to rounding, see implementation.
            if (Double.isInfinite(decoded)) {
                var sign = decoded == Double.POSITIVE_INFINITY ? 1 : -1;
                return sign * Double.MAX_VALUE;
            }

            long reencoded = Math.round(decoded * scalingFactor);
            if (encoded != reencoded) {
                if (encoded > reencoded) {
                    return decoded + Math.ulp(decoded);
                }
                return decoded - Math.ulp(decoded);
            }
            return decoded;
        }

        private double roundDocValues(double d) {
            // Special case due to rounding, see implementation.
            if (Math.abs(d) == Double.MAX_VALUE) {
                return d;
            }

            long encoded = Math.round(d * scalingFactor);
            return encoded * (1 / scalingFactor);
        }

        private void mapping(XContentBuilder b) throws IOException {
            b.field("type", "scaled_float");
            b.field("scaling_factor", scalingFactor);
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
                b.field("ignore_malformed", true);
            }
        }

        @Override
        public List<SyntheticSourceInvalidExample> invalidExample() throws IOException {
            return List.of();
        }
    }

    @Override
    protected Function<Object, Object> loadBlockExpected() {
        return v -> (Number) v;
    }

    @Override
    protected Matcher<?> blockItemMatcher(Object expected) {
        return "NaN".equals(expected) ? notANumber() : equalTo(expected);
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }

    @Override
    protected List<NumberTypeOutOfRangeSpec> outOfRangeSpecs() {
        // No outOfRangeSpecs are specified because ScaledFloatFieldMapper doesn't extend NumberFieldMapper and doesn't use a
        // NumberFieldMapper.NumberType that is present in OutOfRangeSpecs
        return Collections.emptyList();
    }

    @Override
    public void testIgnoreMalformedWithObject() {} // TODO: either implement this, remove it, or update ScaledFloatFieldMapper's behaviour

    @Override
    public void testAllowMultipleValuesField() {} // TODO: either implement this, remove it, or update ScaledFloatFieldMapper's behaviour

    @Override
    public void testScriptableTypes() {} // TODO: either implement this, remove it, or update ScaledFloatFieldMapper's behaviour

    @Override
    public void testDimension() {} // TODO: either implement this, remove it, or update ScaledFloatFieldMapper's behaviour

    @Override
    protected Number missingValue() {
        return 0.123;
    }

    @Override
    protected Number randomNumber() {
        /*
         * The source parser and doc values round trip will both reduce
         * the precision to 32 bits if the value is more precise.
         * randomDoubleBetween will smear the values out across a wide
         * range of valid values.
         */
        return randomBoolean() ? randomDoubleBetween(-Float.MAX_VALUE, Float.MAX_VALUE, true) : randomFloat();
    }

    public void testEncodeDecodeExactScalingFactor() {
        double v = randomValue();
        double expected = 1 / v;
        // We don't produce infinities while decoding. See #testDecodeHandlingInfinity().
        if (Double.isInfinite(expected)) {
            var sign = expected == Double.POSITIVE_INFINITY ? 1 : -1;
            expected = sign * Double.MAX_VALUE;
        }
        assertThat(encodeDecode(1 / v, v), equalTo(expected));
    }

    /**
     * Tests numbers that can be encoded and decoded without saturating a {@code long}.
     */
    public void testEncodeDecodeNoSaturation() {
        double scalingFactor = randomValue();
        double unsaturated = randomDoubleBetween(Long.MIN_VALUE / scalingFactor, Long.MAX_VALUE / scalingFactor, true);
        assertEquals(
            encodeDecode(unsaturated, scalingFactor),
            Math.round(unsaturated * scalingFactor) / scalingFactor,
            unsaturated * 1e-10
        );
    }

    /**
     * Tests that numbers whose encoded value is {@code Long.MIN_VALUE} can be round
     * tripped through synthetic source.
     */
    public void testEncodeDecodeSaturatedLow() {
        double scalingFactor = randomValueOtherThanMany(d -> Double.isInfinite(Long.MIN_VALUE / d), ESTestCase::randomDouble);
        double min = Long.MIN_VALUE / scalingFactor;
        if (min * scalingFactor != Long.MIN_VALUE) {
            min -= Math.ulp(min);
        }
        assertThat(ScaledFloatFieldMapper.encode(min, scalingFactor), equalTo(Long.MIN_VALUE));
        assertThat(encodeDecode(min, scalingFactor), equalTo(min));

        double saturated = randomDoubleBetween(-Double.MAX_VALUE, min, true);
        assertThat(ScaledFloatFieldMapper.encode(saturated, scalingFactor), equalTo(Long.MIN_VALUE));
        assertThat(encodeDecode(saturated, scalingFactor), equalTo(min));
    }

    /**
     * Tests that numbers whose encoded value is {@code Long.MAX_VALUE} can be round
     * tripped through synthetic source.
     */
    public void testEncodeDecodeSaturatedHigh() {
        double scalingFactor = randomValueOtherThanMany(d -> Double.isInfinite(Long.MAX_VALUE / d), ESTestCase::randomDouble);
        double max = Long.MAX_VALUE / scalingFactor;
        if (max * scalingFactor != Long.MAX_VALUE) {
            max += Math.ulp(max);
        }
        assertThat(ScaledFloatFieldMapper.encode(max, scalingFactor), equalTo(Long.MAX_VALUE));
        assertThat(encodeDecode(max, scalingFactor), equalTo(max));

        double saturated = randomDoubleBetween(max, Double.MAX_VALUE, true);
        assertThat(ScaledFloatFieldMapper.encode(saturated, scalingFactor), equalTo(Long.MAX_VALUE));
        assertThat(encodeDecode(saturated, scalingFactor), equalTo(max));
    }

    public void testEncodeDecodeRandom() {
        double scalingFactor = randomDoubleBetween(0, Double.MAX_VALUE, false);
        double v = randomValue();
        double once = encodeDecode(v, scalingFactor);
        double twice = encodeDecode(once, scalingFactor);
        assertThat(twice, equalTo(once));
    }

    /**
     * Tests that a value and scaling factor that won't
     * properly round trip without a "nudge" to keep
     * them from rounding in the wrong direction on the
     * second iteration.
     */
    public void testEncodeDecodeNeedNudge() {
        double scalingFactor = 2.4206374697469164E16;
        double v = 0.15527719259262085;
        double once = encodeDecode(v, scalingFactor);
        double twice = encodeDecode(once, scalingFactor);
        assertThat(twice, equalTo(once));
    }

    /**
     * Tests that any encoded value with that can that fits in the mantissa of
     * a double precision floating point can be round tripped through synthetic
     * source. Values that do not fit in the mantissa will get floating point
     * rounding errors.
     */
    public void testDecodeEncode() {
        double scalingFactor = randomValueOtherThanMany(d -> Double.isInfinite(Long.MAX_VALUE / d), ESTestCase::randomDouble);
        long encoded = randomLongBetween(-2 << 53, 2 << 53);
        assertThat(
            ScaledFloatFieldMapper.encode(ScaledFloatFieldMapper.decodeForSyntheticSource(encoded, scalingFactor), scalingFactor),
            equalTo(encoded)
        );
    }

    /**
     * Tests the case when decoded value is infinite due to rounding.
     */
    public void testDecodeHandlingInfinity() {
        for (var sign : new long[] { 1, -1 }) {
            long encoded = 101;
            double encodedNoRounding = 100.5;
            assertEquals(encoded, Math.round(encodedNoRounding));

            var signedMax = sign * Double.MAX_VALUE;
            // We need a scaling factor that will
            // 1. make encoded long small resulting in significant loss of precision due to rounding
            // 2. result in long value being rounded in correct direction.
            //
            // So we take a scaling factor that would put us right at MAX_VALUE
            // without rounding and hence go beyond MAX_VALUE with rounding.
            double scalingFactor = (encodedNoRounding / signedMax);

            assertThat(ScaledFloatFieldMapper.decodeForSyntheticSource(encoded, scalingFactor), equalTo(signedMax));
        }
    }

    private double encodeDecode(double value, double scalingFactor) {
        return ScaledFloatFieldMapper.decodeForSyntheticSource(ScaledFloatFieldMapper.encode(value, scalingFactor), scalingFactor);
    }

    private static double randomValue() {
        return randomBoolean() ? randomDoubleBetween(-Double.MAX_VALUE, Double.MAX_VALUE, true) : randomFloat();
    }
}
