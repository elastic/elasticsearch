/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ScaledFloatFieldMapperTests extends MapperTestCase {

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
        checker.registerUpdateCheck(
            b -> b.field("ignore_malformed", true),
            m -> assertTrue(((ScaledFloatFieldMapper) m).ignoreMalformed())
        );
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
        XContentBuilder mapping = fieldMapping(b -> b.field("type", "scaled_float").field("scaling_factor", 10.0));
        DocumentMapper mapper = createDocumentMapper(mapping);
        assertEquals(Strings.toString(mapping), mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(source(b -> b.field("field", 123)));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        assertFalse(pointField.fieldType().stored());
        assertEquals(1230, pointField.numericValue().longValue());
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertEquals(1230, dvField.numericValue().longValue());
        assertFalse(dvField.fieldType().stored());
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

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField dvField = fields[0];
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

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        assertEquals(1230, pointField.numericValue().longValue());
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

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(3, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        assertEquals(1230, pointField.numericValue().doubleValue(), 0d);
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        IndexableField storedField = fields[2];
        assertTrue(storedField.fieldType().stored());
        assertEquals(1230, storedField.numericValue().longValue());
    }

    public void testCoerce() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", "123").endObject()),
                XContentType.JSON
            )
        );
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        assertEquals(1230, pointField.numericValue().longValue());
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());

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
        MapperParsingException e = expectThrows(MapperParsingException.class, runnable);
        assertThat(e.getCause().getMessage(), containsString("passed as String"));
    }

    public void testIgnoreMalformed() throws Exception {
        doTestIgnoreMalformed("a", "For input string: \"a\"");

        List<String> values = Arrays.asList("NaN", "Infinity", "-Infinity");
        for (String value : values) {
            doTestIgnoreMalformed(value, "[scaled_float] only supports finite values, but got [" + value + "]");
        }
    }

    private void doTestIgnoreMalformed(String value, String exceptionMessageContains) throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ThrowingRunnable runnable = () -> mapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", value).endObject()),
                XContentType.JSON
            )
        );
        MapperParsingException e = expectThrows(MapperParsingException.class, runnable);
        assertThat(e.getCause().getMessage(), containsString(exceptionMessageContains));

        DocumentMapper mapper2 = createDocumentMapper(
            fieldMapping(b -> b.field("type", "scaled_float").field("scaling_factor", 10.0).field("ignore_malformed", true))
        );
        ParsedDocument doc = mapper2.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", value).endObject()),
                XContentType.JSON
            )
        );

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(0, fields.length);
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
        assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field"));

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
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField pointField = fields[0];
        assertEquals(1, pointField.fieldType().pointDimensionCount());
        assertFalse(pointField.fieldType().stored());
        assertEquals(25, pointField.numericValue().longValue());
        IndexableField dvField = fields[1];
        assertEquals(DocValuesType.SORTED_NUMERIC, dvField.fieldType().docValuesType());
        assertFalse(dvField.fieldType().stored());
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
    protected SyntheticSourceSupport syntheticSourceSupport() {
        return new SyntheticSourceSupport() {
            private final double scalingFactor = randomDoubleBetween(0, Double.MAX_VALUE, false);
            private final Double nullValue = usually() ? null : round(randomValue());

            @Override
            public SyntheticSourceExample example(int maxValues) {
                if (randomBoolean()) {
                    Tuple<Double, Double> v = generateValue();
                    return new SyntheticSourceExample(v.v1(), v.v2(), this::mapping);
                }
                List<Tuple<Double, Double>> values = randomList(1, maxValues, this::generateValue);
                List<Double> in = values.stream().map(Tuple::v1).toList();
                List<Double> outList = values.stream().map(Tuple::v2).sorted().toList();
                Object out = outList.size() == 1 ? outList.get(0) : outList;
                return new SyntheticSourceExample(in, out, this::mapping);
            }

            private Tuple<Double, Double> generateValue() {
                if (nullValue != null && randomBoolean()) {
                    return Tuple.tuple(null, nullValue);
                }
                double d = randomValue();
                return Tuple.tuple(d, round(d));
            }

            private double round(double d) {
                long encoded = Math.round(d * scalingFactor);
                double decoded = encoded / scalingFactor;
                long reencoded = Math.round(decoded * scalingFactor);
                if (encoded != reencoded) {
                    if (encoded > reencoded) {
                        return decoded + Math.ulp(decoded);
                    }
                    return decoded - Math.ulp(decoded);
                }
                return decoded;
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
            }

            @Override
            public List<SyntheticSourceInvalidExample> invalidExample() throws IOException {
                return List.of(
                    new SyntheticSourceInvalidExample(
                        equalTo("field [field] of type [scaled_float] doesn't support synthetic source because it doesn't have doc values"),
                        b -> b.field("type", "scaled_float").field("scaling_factor", 10).field("doc_values", false)
                    ),
                    new SyntheticSourceInvalidExample(
                        equalTo(
                            "field [field] of type [scaled_float] doesn't support synthetic source because it ignores malformed numbers"
                        ),
                        b -> b.field("type", "scaled_float").field("scaling_factor", 10).field("ignore_malformed", true)
                    )
                );
            }
        };
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }

    public void testEncodeDecodeExactScalingFactor() {
        double v = randomValue();
        assertThat(encodeDecode(1 / v, v), equalTo(1 / v));
    }

    /**
     * Tests numbers that can be encoded and decoded without saturating a {@code long}.
     */
    public void testEncodeDecodeNoSaturation() {
        double scalingFactor = randomValue();
        double unsaturated = randomDoubleBetween(Long.MIN_VALUE / scalingFactor, Long.MAX_VALUE / scalingFactor, true);
        assertThat(encodeDecode(unsaturated, scalingFactor), equalTo(Math.round(unsaturated * scalingFactor) / scalingFactor));
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
     * Tests that numbers whose encoded value is {@code Long.MIN_VALUE} can be round
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

    private double encodeDecode(double value, double scalingFactor) {
        return ScaledFloatFieldMapper.decodeForSyntheticSource(ScaledFloatFieldMapper.encode(value, scalingFactor), scalingFactor);
    }

    private double randomValue() {
        return randomBoolean() ? randomDoubleBetween(-Double.MAX_VALUE, Double.MAX_VALUE, true) : randomFloat();
    }
}
