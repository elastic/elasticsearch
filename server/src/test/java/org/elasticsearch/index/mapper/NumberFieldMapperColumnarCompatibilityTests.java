/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Parity tests for {@link NumberFieldMapper#mapColumnBatch} against the row path. Single-valued
 * fields get one test per numeric type and ESCF source kind combination; multi-valued fields are
 * covered by a property test over type, array shape, index profile and encoder. Absent (sparse)
 * docs are exercised in every scenario to confirm validity-bitset handling.
 */
public class NumberFieldMapperColumnarCompatibilityTests extends AbstractColumnarMapperCompatibilityTestCase {

    private static final String FIELD = "f";

    /**
     * Columnar-mode settings that satisfy {@link NumberFieldMapper#supportsColumnarParse}:
     * single-value doc-values ({@code multi_value=false}).
     */
    private static Settings columnarSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
            .put(FieldMapper.DOC_VALUES_MULTI_VALUE_SETTING.getKey(), false)
            .build();
    }

    public void testLongField_singleValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "long").endObject()),
            columnarSettings(),
            batch("long single-value", 1L, doc("d1", 1L, "{\"f\":42}"))
        );
    }

    public void testLongField_absentDocs() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "long").endObject()),
            columnarSettings(),
            batch("long absent", 1L, doc("d1", 1L, "{\"f\":1}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":3}"))
        );
    }

    public void testLongField_negative() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "long").endObject()),
            columnarSettings(),
            batch("long negative", 1L, doc("d1", 1L, "{\"f\":-9223372036854775808}"), doc("d2", 2L, "{\"f\":9223372036854775807}"))
        );
    }

    public void testIntegerField_singleValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "integer").endObject()),
            columnarSettings(),
            batch("integer single-value", 1L, doc("d1", 1L, "{\"f\":100}"))
        );
    }

    public void testIntegerField_absentDocs() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "integer").endObject()),
            columnarSettings(),
            batch("integer absent", 1L, doc("d1", 1L, "{\"f\":10}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":20}"))
        );
    }

    public void testShortField_singleValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "short").endObject()),
            columnarSettings(),
            batch("short single-value", 1L, doc("d1", 1L, "{\"f\":32767}"))
        );
    }

    public void testShortField_absentDocs() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "short").endObject()),
            columnarSettings(),
            batch("short absent", 1L, doc("d1", 1L, "{\"f\":1}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":-1}"))
        );
    }

    public void testByteField_singleValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "byte").endObject()),
            columnarSettings(),
            batch("byte single-value", 1L, doc("d1", 1L, "{\"f\":127}"))
        );
    }

    public void testByteField_absentDocs() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "byte").endObject()),
            columnarSettings(),
            batch("byte absent", 1L, doc("d1", 1L, "{\"f\":-128}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":0}"))
        );
    }

    public void testLongField_indexed() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "long").field("index", true).endObject()),
            columnarSettings(),
            batch("long indexed", 1L, doc("d1", 1L, "{\"f\":42}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":-7}"))
        );
    }

    public void testIntegerField_indexed() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "integer").field("index", true).endObject()),
            columnarSettings(),
            batch("integer indexed", 1L, doc("d1", 1L, "{\"f\":100}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":-50}"))
        );
    }

    public void testShortField_indexed() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "short").field("index", true).endObject()),
            columnarSettings(),
            batch("short indexed", 1L, doc("d1", 1L, "{\"f\":32767}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":-1}"))
        );
    }

    public void testByteField_indexed() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "byte").field("index", true).endObject()),
            columnarSettings(),
            batch("byte indexed", 1L, doc("d1", 1L, "{\"f\":127}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":-128}"))
        );
    }

    public void testFloatField_indexed() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "float").field("index", true).endObject()),
            columnarSettings(),
            batch("float indexed", 1L, doc("d1", 1L, "{\"f\":1.5}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":-2.25}"))
        );
    }

    public void testDoubleField_indexed() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "double").field("index", true).endObject()),
            columnarSettings(),
            batch("double indexed", 1L, doc("d1", 1L, "{\"f\":1.5}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":-2.25}"))
        );
    }

    public void testHalfFloatField_indexed() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "half_float").field("index", true).endObject()),
            columnarSettings(),
            batch("half_float indexed", 1L, doc("d1", 1L, "{\"f\":1.5}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":-2.25}"))
        );
    }

    public void testFloatField_doubleColumn() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "float").endObject()),
            columnarSettings(),
            batch("float from double", 1L, doc("d1", 1L, "{\"f\":1.5}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":-2.25}"))
        );
    }

    /** JSON integer values encode as LONG in ESCF; the mapper converts via {@code floatToSortableInt}. */
    public void testFloatField_longColumn() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "float").endObject()),
            columnarSettings(),
            batch("float from long", 1L, doc("d1", 1L, "{\"f\":5}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":-100}"))
        );
    }

    public void testDoubleField_doubleColumn() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "double").endObject()),
            columnarSettings(),
            batch("double from double", 1L, doc("d1", 1L, "{\"f\":1.5}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":-2.25}"))
        );
    }

    /** JSON integer values encode as LONG in ESCF; the mapper converts via {@code doubleToSortableLong}. */
    public void testDoubleField_longColumn() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "double").endObject()),
            columnarSettings(),
            batch("double from long", 1L, doc("d1", 1L, "{\"f\":5}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":-100}"))
        );
    }

    public void testLongField_stringColumn() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "long").endObject()),
            columnarSettings(),
            batch("long string", 1L, doc("d1", 1L, "{\"f\":\"42\"}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":\"-7\"}"))
        );
    }

    /** Quoted Long.MIN_VALUE and Long.MAX_VALUE exercise the ASCII fast path at boundary values. */
    public void testLongField_stringColumn_boundaries() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "long").endObject()),
            columnarSettings(),
            batch(
                "long string boundaries",
                1L,
                doc("d1", 1L, "{\"f\":\"-9223372036854775808\"}"),
                doc("d2", 2L, "{\"f\":\"9223372036854775807\"}")
            )
        );
    }

    /**
     * A batch mixing a plain integer string (ASCII fast path) and scientific notation (slow path)
     * must produce the same doc values as the row path for both.
     */
    public void testLongField_stringColumn_fastPathAndFallbackInOneBatch() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "long").endObject()),
            columnarSettings(),
            batch("long string fast+slow", 1L, doc("d1", 1L, "{\"f\":\"1000\"}"), doc("d2", 2L, "{\"f\":\"1e3\"}"))
        );
    }

    /** A decimal string with coerce=true (default) is truncated to a long, matching the row path. */
    public void testLongField_stringColumn_decimalTruncated() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "long").endObject()),
            columnarSettings(),
            batch("long string decimal coerce", 1L, doc("d1", 1L, "{\"f\":\"1.9\"}"), doc("d2", 2L, "{\"f\":\"42\"}"))
        );
    }

    public void testLongField_stringColumn_emptyStringMissing() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "long").endObject()),
            columnarSettings(),
            batch("long empty string missing", 1L, doc("d1", 1L, "{\"f\":\"10\"}"), doc("d2", 2L, "{\"f\":\"\"}"), doc("d3", 3L, "{}"))
        );
    }

    public void testLongField_stringColumn_emptyStringUsesNullValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "long").field("null_value", 7).endObject()),
            columnarSettings(),
            batch("long empty string null_value", 1L, doc("d1", 1L, "{\"f\":\"10\"}"), doc("d2", 2L, "{\"f\":\"\"}"), doc("d3", 3L, "{}"))
        );
    }

    public void testLongField_stringColumn_coerceFalseRejectsNumericString() throws IOException {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> assertColumnarMatchesXContent(
                mapping(b -> b.startObject(FIELD).field("type", "long").field("coerce", false).endObject()),
                columnarSettings(),
                batch("long coerce false string", 1L, doc("d1", 1L, "{\"f\":\"42\"}"))
            )
        );
        assertTrue("expected coerce message but got: " + ex.getMessage(), ex.getMessage().contains("Long value passed as String"));
    }

    public void testLongField_stringColumn_coerceFalseRejectsEmptyString() throws IOException {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> assertColumnarMatchesXContent(
                mapping(b -> b.startObject(FIELD).field("type", "long").field("coerce", false).endObject()),
                columnarSettings(),
                batch("long coerce false empty string", 1L, doc("d1", 1L, "{\"f\":\"\"}"))
            )
        );
        assertTrue("expected coerce message but got: " + ex.getMessage(), ex.getMessage().contains("Long value passed as String"));
    }

    public void testDoubleField_bigIntegerNumberTokenStoredAsStringColumn() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "double").endObject()),
            columnarSettings(),
            batch("double big integer token", 1L, doc("d1", 1L, "{\"f\":9223372036854775808}"))
        );
    }

    public void testDoubleField_bigDecimalNumberTokenStoredAsStringColumn() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "double").endObject()),
            columnarSettings(),
            batch("double big decimal token", 1L, doc("d1", 1L, "{\"f\":1.2345678901234567890123456789}"))
        );
    }

    public void testIntegerField_stringColumn() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "integer").endObject()),
            columnarSettings(),
            batch("integer string", 1L, doc("d1", 1L, "{\"f\":\"100\"}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":\"-50\"}"))
        );
    }

    public void testIntegerField_stringColumn_decimalTruncated() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "integer").endObject()),
            columnarSettings(),
            batch("integer string decimal coerce", 1L, doc("d1", 1L, "{\"f\":\"123.9\"}"), doc("d2", 2L, "{\"f\":\"-123.9\"}"))
        );
    }

    public void testShortField_stringColumn() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "short").endObject()),
            columnarSettings(),
            batch("short string", 1L, doc("d1", 1L, "{\"f\":\"32767\"}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":\"-1\"}"))
        );
    }

    public void testByteField_stringColumn() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "byte").endObject()),
            columnarSettings(),
            batch("byte string", 1L, doc("d1", 1L, "{\"f\":\"127\"}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":\"-128\"}"))
        );
    }

    public void testFloatField_stringColumn() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "float").endObject()),
            columnarSettings(),
            batch("float string", 1L, doc("d1", 1L, "{\"f\":\"1.5\"}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":\"-2.25\"}"))
        );
    }

    public void testDoubleField_stringColumn() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "double").endObject()),
            columnarSettings(),
            batch("double string", 1L, doc("d1", 1L, "{\"f\":\"1.5\"}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":\"-2.25\"}"))
        );
    }

    public void testHalfFloatField_stringColumn() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "half_float").endObject()),
            columnarSettings(),
            batch("half_float string", 1L, doc("d1", 1L, "{\"f\":\"1.5\"}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":\"-2.25\"}"))
        );
    }

    /**
     * Columnar-mode settings leaving {@code doc_values.multi_value} at its default of {@code true},
     * so array values reach the mapper instead of being rejected at parse time.
     */
    private static Settings multiValueColumnarSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
            .build();
    }

    /**
     * {@link NumberFieldMapper#supportsColumnarParse} accepts {@code doc_values.multi_value=true} —
     * the setting defaults to {@code true}, so rejecting it would take every numeric field in a
     * columnar index off the columnar path. Multi-valued documents arrive as an ESCF {@code ARRAY}
     * column, which {@link NumberFieldMapper#mapColumnBatch} maps into a sortable-long array column
     * plus the positional offsets sidecar the row path records for synthetic source.
     *
     * <p>Which column kind a multi-valued document produces follows the JSON literals rather than the
     * mapping: the encoder classifies every element by width, and an array whose elements land in
     * different width classes packs as a {@code UNION} the mapper refuses, falling the batch back to
     * the row path. This walks every numeric type against every applicable {@link ArrayShape}, in both
     * {@link IndexProfile}s, under every available encoder, and asserts the outcome
     * {@link #expectedOutcome} declares — so a shape moving between parity and fallback in either
     * direction fails here.
     *
     * <p>One cell of that table is asymmetric on purpose: the encoders disagree about
     * {@link ArrayShape#MIXED_NUMERIC_WIDTH} on floating-point types, because Jackson reports every
     * JSON float literal as {@code DOUBLE} while simdjson classifies each value on whether it
     * round-trips through {@code float}. Reconciling them is follow-up work in the encoder, after
     * which the floating-point entry for that shape reads {@code EQUAL} under both.
     */
    public void testMultiValueShapesAcrossEncoders() throws IOException {
        for (NumberType type : NumberType.values()) {
            for (ArrayShape shape : ArrayShape.values()) {
                if (shape.appliesTo(type) == false) {
                    continue;
                }
                for (IndexProfile profile : IndexProfile.values()) {
                    for (SourceEncoder encoder : SourceEncoder.available()) {
                        assertShape(
                            new ShapeCase(
                                type,
                                shape,
                                profile,
                                encoder,
                                profile.allowsStore() && randomBoolean(),
                                randomBoolean(),
                                randomSources(profile, type, shape)
                            )
                        );
                    }
                }
            }
        }
    }

    private enum Outcome {
        EQUAL,
        FALLBACK_UNION
    }

    /**
     * The index configurations a numeric field can take the columnar path in. Columnar mode rejects
     * {@code store} at mapping-parse time, so stored multi-value coverage has to run in time-series
     * mode, which brings its own {@code @timestamp} and dimension requirements.
     */
    private enum IndexProfile {
        COLUMNAR,
        TSDB;

        boolean allowsStore() {
            return switch (this) {
                case COLUMNAR -> false;
                case TSDB -> true;
            };
        }

        Settings settings() {
            return switch (this) {
                case COLUMNAR -> multiValueColumnarSettings();
                case TSDB -> tsdbSettings();
            };
        }
    }

    /**
     * The element-type mix inside a multi-valued document. Shapes are named after the encoder's own
     * width classes — {@code INT} against {@code LONG} for integral types, float-representable
     * against double-only for floating ones — because those classes, not the field mapping, decide
     * whether an array packs as a fixed array or as a union.
     */
    private enum ArrayShape {
        HOMOGENEOUS_NUMBER,
        MIXED_NUMERIC_WIDTH,
        NUMERIC_STRINGS,
        MIXED_NUMBER_STRING;

        /**
         * Whether the type's own range admits two width classes. Every value a {@code byte},
         * {@code short} or {@code integer} field accepts falls in the narrow class, so a mixed-width
         * array is unreachable for them without going out of range, which both paths reject anyway.
         */
        boolean appliesTo(NumberType type) {
            return switch (this) {
                case HOMOGENEOUS_NUMBER, NUMERIC_STRINGS, MIXED_NUMBER_STRING -> true;
                case MIXED_NUMERIC_WIDTH -> switch (type) {
                    case BYTE, SHORT, INTEGER -> false;
                    case LONG, HALF_FLOAT, FLOAT, DOUBLE -> true;
                };
            };
        }
    }

    private record ShapeCase(
        NumberType type,
        ArrayShape shape,
        IndexProfile profile,
        SourceEncoder encoder,
        boolean stored,
        boolean indexed,
        List<String> sources
    ) {
        @Override
        public String toString() {
            return type.typeName()
                + " "
                + shape
                + " profile="
                + profile
                + " encoder="
                + encoder
                + " stored="
                + stored
                + " indexed="
                + indexed
                + " sources="
                + sources;
        }
    }

    private static Outcome expectedOutcome(NumberType type, ArrayShape shape, SourceEncoder encoder) {
        return switch (shape) {
            case HOMOGENEOUS_NUMBER, NUMERIC_STRINGS -> Outcome.EQUAL;
            case MIXED_NUMBER_STRING -> Outcome.FALLBACK_UNION;
            case MIXED_NUMERIC_WIDTH -> switch (type) {
                case LONG -> Outcome.FALLBACK_UNION;
                case HALF_FLOAT, FLOAT, DOUBLE -> switch (encoder) {
                    case JACKSON -> Outcome.EQUAL;
                    case SIMD -> Outcome.FALLBACK_UNION;
                };
                case BYTE, SHORT, INTEGER -> throw new AssertionError(shape + " does not apply to " + type);
            };
        };
    }

    private void assertShape(ShapeCase testCase) throws IOException {
        switch (expectedOutcome(testCase.type(), testCase.shape(), testCase.encoder())) {
            case EQUAL -> assertColumnarMatchesXContent(
                mapping(shapeMapping(testCase)),
                testCase.profile().settings(),
                testCase.encoder(),
                batch(testCase.toString(), 1L, shapeDocs(testCase))
            );
            case FALLBACK_UNION -> {
                final MapperService mapperService = createMapperService(testCase.profile().settings(), mapping(shapeMapping(testCase)));
                final UnsupportedOperationException ex = expectThrows(
                    UnsupportedOperationException.class,
                    testCase.toString(),
                    () -> mapColumnarLeaf(mapperService, FIELD, testCase.encoder(), testCase.sources().toArray(String[]::new))
                );
                assertTrue(
                    testCase + ": expected a UNION refusal but got: " + ex.getMessage(),
                    ex.getMessage().contains("ESCF column kind [UNION]")
                );
            }
        }
    }

    private static CheckedConsumer<XContentBuilder, IOException> shapeMapping(ShapeCase testCase) {
        return b -> {
            if (testCase.profile() == IndexProfile.TSDB) {
                b.startObject("@timestamp").field("type", "date").endObject();
                b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            }
            b.startObject(FIELD).field("type", testCase.type().typeName());
            if (testCase.stored()) {
                b.field("store", true);
            }
            if (testCase.indexed()) {
                b.field("index", true);
            }
            b.endObject();
        };
    }

    private static Doc[] shapeDocs(ShapeCase testCase) {
        final List<String> sources = testCase.sources();
        final Doc[] docs = new Doc[sources.size()];
        for (int i = 0; i < docs.length; i++) {
            docs[i] = switch (testCase.profile()) {
                case COLUMNAR -> doc("d" + i, i + 1L, sources.get(i));
                // The tsid the coordinator would have computed is handed to both paths, as the other
                // time-series scenarios do; only the timestamp varies, to keep the _id values distinct.
                case TSDB -> doc(
                    TsidExtractingIdFieldMapper.createId(ST_ROUTING_HASH, ST_TSID, ST_TS_A + i),
                    ST_ROUTING,
                    ST_TSID,
                    i + 1L,
                    sources.get(i)
                );
            };
        }
        return docs;
    }

    /**
     * Complete JSON sources for one shape. The first document is always multi-valued so the shape
     * reaches the column kind it is named for; the rest vary between multi-valued, single-element and
     * absent to keep the validity bitset and the single-slot offsets decision in play.
     */
    private static List<String> randomSources(IndexProfile profile, NumberType type, ArrayShape shape) {
        final int docCount = randomIntBetween(2, 4);
        final List<String> sources = new ArrayList<>(docCount);
        sources.add(source(profile, 0, arrayField(type, shape, randomIntBetween(2, 4))));
        for (int i = 1; i < docCount; i++) {
            final String field = switch (randomIntBetween(0, 2)) {
                case 0 -> arrayField(type, shape, randomIntBetween(2, 4));
                case 1 -> arrayField(type, shape, 1);
                case 2 -> "";
                default -> throw new AssertionError("unreachable");
            };
            sources.add(source(profile, i, field));
        }
        return sources;
    }

    private static String source(IndexProfile profile, int docIndex, String field) {
        return switch (profile) {
            case COLUMNAR -> "{" + field + "}";
            case TSDB -> "{\"@timestamp\":" + (ST_TS_A + docIndex) + (field.isEmpty() ? "" : "," + field) + "}";
        };
    }

    private static String arrayField(NumberType type, ArrayShape shape, int elementCount) {
        return "\"" + FIELD + "\":[" + String.join(",", elements(type, shape, elementCount)) + "]";
    }

    private static List<String> elements(NumberType type, ArrayShape shape, int elementCount) {
        final List<String> elements = new ArrayList<>(elementCount);
        switch (shape) {
            case HOMOGENEOUS_NUMBER -> {
                final boolean wide = ArrayShape.MIXED_NUMERIC_WIDTH.appliesTo(type) && randomBoolean();
                for (int i = 0; i < elementCount; i++) {
                    elements.add(wide ? wideLiteral(type) : narrowLiteral(type));
                }
            }
            case MIXED_NUMERIC_WIDTH -> {
                elements.add(narrowLiteral(type));
                if (elementCount > 1) {
                    elements.add(wideLiteral(type));
                }
                for (int i = 2; i < elementCount; i++) {
                    elements.add(randomBoolean() ? narrowLiteral(type) : wideLiteral(type));
                }
            }
            case NUMERIC_STRINGS -> {
                for (int i = 0; i < elementCount; i++) {
                    elements.add(quoted(narrowLiteral(type)));
                }
            }
            case MIXED_NUMBER_STRING -> {
                elements.add(narrowLiteral(type));
                if (elementCount > 1) {
                    elements.add(quoted(narrowLiteral(type)));
                }
                for (int i = 2; i < elementCount; i++) {
                    elements.add(randomBoolean() ? narrowLiteral(type) : quoted(narrowLiteral(type)));
                }
            }
        }
        return elements;
    }

    private static String quoted(String literal) {
        return "\"" + literal + "\"";
    }

    /** A literal in the type's narrow width class: {@code INT} for integral types, float-exact for floating ones. */
    private static String narrowLiteral(NumberType type) {
        return switch (type) {
            case BYTE -> Integer.toString(randomIntBetween(Byte.MIN_VALUE, Byte.MAX_VALUE));
            case SHORT -> Integer.toString(randomIntBetween(Short.MIN_VALUE, Short.MAX_VALUE));
            case INTEGER, LONG -> Integer.toString(randomIntBetween(-1000, 1000));
            // Halves are exact in float and in half_float, so the encoder classifies them as FLOAT.
            case HALF_FLOAT, FLOAT, DOUBLE -> Double.toString(randomIntBetween(-1000, 1000) + 0.5);
        };
    }

    /** A literal in the type's wide width class: beyond {@code int} range, or not representable as a {@code float}. */
    private static String wideLiteral(NumberType type) {
        return switch (type) {
            case LONG -> Long.toString(randomLongBetween(Integer.MAX_VALUE + 1L, Long.MAX_VALUE / 2));
            case HALF_FLOAT, FLOAT, DOUBLE -> randomFrom("0.1", "2.718281828", "1.2345678901234567");
            case BYTE, SHORT, INTEGER -> throw new AssertionError("no wide literal fits in " + type);
        };
    }

    /**
     * An ARRAY column whose child is STRING and whose every row holds exactly one element. The
     * string transform writes one value per document into a scalar-locked builder, so the output
     * column is a plain LONG rather than an ARRAY even though the source was an ARRAY. The row path
     * likewise records no offsets sidecar for single-slot rows, so parity holds.
     */
    public void testLongField_multiValue_singleElementStringArrays() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "long").endObject()),
            multiValueColumnarSettings(),
            batch("long single-element string arrays", 1L, doc("d1", 1L, "{\"f\":[\"1\"]}"), doc("d2", 2L, "{\"f\":[\"2\"]}"))
        );
    }

    /**
     * Every row is a single-element numeric array, so the transform keeps an ARRAY column but no row
     * has enough slots to be worth recording. The sidecar builder returns nothing, matching the row
     * path's per-document decision to skip offsets for single-slot rows.
     */
    public void testLongField_multiValue_allSingleElementNoOffsets() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "long").endObject()),
            multiValueColumnarSettings(),
            batch("long all single-element", 1L, doc("d1", 1L, "{\"f\":[1]}"), doc("d2", 2L, "{}"), doc("d3", 3L, "{\"f\":[2]}"))
        );
    }

    /**
     * Doc values keep each document's values sorted and deduplicated, so repeated and out-of-order elements are
     * the case only the offsets sidecar can reconstruct. Both paths must record the same per-slot ordinals.
     */
    public void testLongField_multiValue_duplicatesAndOrder() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "long").endObject()),
            multiValueColumnarSettings(),
            batch(
                "long duplicates",
                1L,
                doc("d1", 1L, "{\"f\":[2,1,2]}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":[5,5,5,5]}"),
                doc("d4", 4L, "{\"f\":[-1,7]}")
            )
        );
    }

    /**
     * An indexed half_float emits its BKD points as a separate 2-byte column, so the points transform has to walk
     * every element of an ARRAY column rather than one value per document.
     */
    public void testHalfFloatField_multiValueIndexed() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "half_float").field("index", true).endObject()),
            multiValueColumnarSettings(),
            batch(
                "half_float multi-value indexed",
                1L,
                doc("d1", 1L, "{\"f\":[1.5,-2.25,3.0]}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":[0.5,0.5]}")
            )
        );
    }

    /** ARRAY-of-STRING: half_float elements parse through the string path and still record the offsets sidecar. */
    public void testHalfFloatField_multiValueStringColumn() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "half_float").endObject()),
            multiValueColumnarSettings(),
            batch(
                "half_float multi-value string",
                1L,
                doc("d1", 1L, "{\"f\":[\"1.5\",\"-2.25\",\"3.0\"]}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":[\"0.5\",\"0.5\"]}")
            )
        );
    }

    /**
     * A null among scalar values makes the column a UNION rather than a plain LONG, which the kind
     * switch in {@link NumberFieldMapper#mapColumnBatch} rejects.
     */
    @AwaitsFix(bugUrl = "columnar mapColumnBatch does not implement null numeric values; UNION columns fall back to the row path")
    public void testLongField_nullValue() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "long").field("null_value", 9).endObject()),
            columnarSettings(),
            batch("long null value", 1L, doc("d1", 1L, "{\"f\":1}"), doc("d2", 2L, "{\"f\":null}"), doc("d3", 3L, "{\"f\":3}"))
        );
    }

    /**
     * {@link NumberFieldMapper#supportsColumnarParse} accepts {@code ignore_malformed=true} — the
     * logsdb index modes default it to {@code true}. Per-value error handling
     * ({@code addIgnoredField} plus the ignored-source stored copy) is not implemented in
     * {@code mapColumnBatch}, so an unparseable value throws out of {@code NumberColumnTransform}
     * and the chunk falls back to the row path, which applies {@code ignore_malformed} properly.
     */
    @AwaitsFix(bugUrl = "columnar mapColumnBatch does not implement ignore_malformed; malformed values fall back to the row path")
    public void testLongField_ignoreMalformed() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "long").field("ignore_malformed", true).endObject()),
            columnarSettings(),
            // All values are strings so the column is a plain STRING and the malformed value reaches
            // the numeric parser; mixing in a JSON number would make it a UNION and fail earlier.
            batch(
                "long ignore_malformed",
                1L,
                doc("d1", 1L, "{\"f\":\"1\"}"),
                doc("d2", 2L, "{\"f\":\"not-a-number\"}"),
                doc("d3", 3L, "{}")
            )
        );
    }

    /**
     * As {@link #testLongField_ignoreMalformed}, for a value that parses but falls outside the
     * type's range — rejected by {@code NumberColumnTransform#validateLongRange} rather than by the
     * string parser.
     */
    @AwaitsFix(bugUrl = "columnar mapColumnBatch does not implement ignore_malformed; out-of-range values fall back to the row path")
    public void testByteField_ignoreMalformedOutOfRange() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "byte").field("ignore_malformed", true).endObject()),
            columnarSettings(),
            batch("byte ignore_malformed out of range", 1L, doc("d1", 1L, "{\"f\":1}"), doc("d2", 2L, "{\"f\":300}"))
        );
    }

    // ---- store=true (TIME_SERIES / TSDB mode) ------------------------------------------------
    //
    // strict-columnar index modes (COLUMNAR, LOGSDB_COLUMNAR) reject store=true at mapping
    // validation time. TIME_SERIES is the only columnar-eligible mode that permits it.
    // These tests use the coordinator-tsid path (index.dimensions) so that metadata fields
    // are computed columnarally via pre-supplied tsid bytes. The keyword dimension field (dim)
    // is declared in the mapping but absent from sources to keep it out of the ESCF schema.

    private static final BytesRef ST_TSID = new BytesRef(new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05 });
    private static final int ST_ROUTING_HASH = 42;
    private static final String ST_ROUTING = TimeSeriesRoutingHashFieldMapper.encode(ST_ROUTING_HASH);
    // epoch millis: 2024-01-15T12:00:00.000Z, 2024-06-01T00:00:00.000Z
    private static final long ST_TS_A = 1705320000000L;
    private static final long ST_TS_B = 1717200000000L;

    private static Settings tsdbSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            .put(IndexMetadata.INDEX_DIMENSIONS.getKey(), "dim")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "-9999-01-01T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "9999-01-01T00:00:00Z")
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false)
            .put(IndexSettings.SYNTHETIC_ID.getKey(), false)
            .build();
    }

    public void testLongField_stored() throws IOException {
        final String idA = TsidExtractingIdFieldMapper.createId(ST_ROUTING_HASH, ST_TSID, ST_TS_A);
        final String idB = TsidExtractingIdFieldMapper.createId(ST_ROUTING_HASH, ST_TSID, ST_TS_B);
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(FIELD).field("type", "long").field("store", true).endObject();
        }),
            tsdbSettings(),
            batch(
                "long stored",
                1L,
                doc(idA, ST_ROUTING, ST_TSID, 1L, "{\"@timestamp\":" + ST_TS_A + ",\"f\":42}"),
                doc(idB, ST_ROUTING, ST_TSID, 2L, "{\"@timestamp\":" + ST_TS_B + "}")
            )
        );
    }

    public void testIntegerField_stored() throws IOException {
        final String idA = TsidExtractingIdFieldMapper.createId(ST_ROUTING_HASH, ST_TSID, ST_TS_A);
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(FIELD).field("type", "integer").field("store", true).endObject();
        }), tsdbSettings(), batch("integer stored", 1L, doc(idA, ST_ROUTING, ST_TSID, 1L, "{\"@timestamp\":" + ST_TS_A + ",\"f\":7}")));
    }

    public void testFloatField_stored() throws IOException {
        final String idA = TsidExtractingIdFieldMapper.createId(ST_ROUTING_HASH, ST_TSID, ST_TS_A);
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(FIELD).field("type", "float").field("store", true).endObject();
        }), tsdbSettings(), batch("float stored", 1L, doc(idA, ST_ROUTING, ST_TSID, 1L, "{\"@timestamp\":" + ST_TS_A + ",\"f\":3.14}")));
    }

    public void testDoubleField_stored() throws IOException {
        final String idA = TsidExtractingIdFieldMapper.createId(ST_ROUTING_HASH, ST_TSID, ST_TS_A);
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(FIELD).field("type", "double").field("store", true).endObject();
        }),
            tsdbSettings(),
            batch("double stored", 1L, doc(idA, ST_ROUTING, ST_TSID, 1L, "{\"@timestamp\":" + ST_TS_A + ",\"f\":2.718281828}"))
        );
    }

    public void testHalfFloatField_stored() throws IOException {
        final String idA = TsidExtractingIdFieldMapper.createId(ST_ROUTING_HASH, ST_TSID, ST_TS_A);
        final String idB = TsidExtractingIdFieldMapper.createId(ST_ROUTING_HASH, ST_TSID, ST_TS_B);
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(FIELD).field("type", "half_float").field("store", true).endObject();
        }),
            tsdbSettings(),
            batch(
                "half_float stored",
                1L,
                doc(idA, ST_ROUTING, ST_TSID, 1L, "{\"@timestamp\":" + ST_TS_A + ",\"f\":1.5}"),
                doc(idB, ST_ROUTING, ST_TSID, 2L, "{\"@timestamp\":" + ST_TS_B + ",\"f\":2.718281828}")
            )
        );
    }

    public void testHalfFloatField_storedLongColumn() throws IOException {
        final String idA = TsidExtractingIdFieldMapper.createId(ST_ROUTING_HASH, ST_TSID, ST_TS_A);
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(FIELD).field("type", "half_float").field("store", true).endObject();
        }),
            tsdbSettings(),
            batch("half_float stored long column", 1L, doc(idA, ST_ROUTING, ST_TSID, 1L, "{\"@timestamp\":" + ST_TS_A + ",\"f\":4097}"))
        );
    }

    public void testHalfFloatField_storedStringColumn() throws IOException {
        final String idA = TsidExtractingIdFieldMapper.createId(ST_ROUTING_HASH, ST_TSID, ST_TS_A);
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(FIELD).field("type", "half_float").field("store", true).endObject();
        }),
            tsdbSettings(),
            batch(
                "half_float stored string column",
                1L,
                doc(idA, ST_ROUTING, ST_TSID, 1L, "{\"@timestamp\":" + ST_TS_A + ",\"f\":\"2.718281828\"}")
            )
        );
    }

    public void testHalfFloatField_storedEmptyStringUsesNullValue() throws IOException {
        final String idA = TsidExtractingIdFieldMapper.createId(ST_ROUTING_HASH, ST_TSID, ST_TS_A);
        final String idB = TsidExtractingIdFieldMapper.createId(ST_ROUTING_HASH, ST_TSID, ST_TS_B);
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(FIELD).field("type", "half_float").field("store", true).field("null_value", 0.1).endObject();
        }),
            tsdbSettings(),
            batch(
                "half_float stored empty string null_value",
                1L,
                doc(idA, ST_ROUTING, ST_TSID, 1L, "{\"@timestamp\":" + ST_TS_A + ",\"f\":\"2.718281828\"}"),
                doc(idB, ST_ROUTING, ST_TSID, 2L, "{\"@timestamp\":" + ST_TS_B + ",\"f\":\"\"}")
            )
        );
    }

    /**
     * The half_float stored column is re-read from the source at float precision rather than derived from the
     * quantized doc values, and has to emit one stored value per array element. Stored fields record no offsets
     * sidecar, so this runs in time-series mode without one.
     */
    public void testHalfFloatField_storedMultiValue() throws IOException {
        final String idA = TsidExtractingIdFieldMapper.createId(ST_ROUTING_HASH, ST_TSID, ST_TS_A);
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(FIELD).field("type", "half_float").field("store", true).endObject();
        }),
            tsdbSettings(),
            batch(
                "half_float stored multi-value",
                1L,
                doc(idA, ST_ROUTING, ST_TSID, 1L, "{\"@timestamp\":" + ST_TS_A + ",\"f\":[1.5,-2.25,1.5]}")
            )
        );
    }

    /**
     * A coerced empty string with no {@code null_value} records a null slot in the row path's offsets sidecar,
     * which the columnar sidecar cannot emit, so both a scalar and an array occurrence fall back to the row path.
     */
    public void testEmptyStringWithOffsetsBailsOutOfColumnarPath() throws IOException {
        final var mapperService = createMapperService(
            multiValueColumnarSettings(),
            mapping(b -> b.startObject(FIELD).field("type", "long").endObject())
        );
        for (String source : List.of("{\"f\":\"\"}", "{\"f\":[\"\",\"5\"]}")) {
            final UnsupportedOperationException ex = expectThrows(
                UnsupportedOperationException.class,
                source,
                () -> mapColumnarLeaf(mapperService, FIELD, source)
            );
            assertTrue(source + ": " + ex.getMessage(), ex.getMessage().contains("records a null offsets slot"));
        }
    }

    /**
     * With a {@code null_value} configured, a coerced empty string inside an array indexes the null value
     * rather than dropping the slot, so the row path records an ordinary ordinal for it and the columnar
     * path can build the offsets sidecar instead of bailing out. The duplicates and out-of-order values
     * make the sidecar carry real ordering information.
     */
    public void testLongField_multiValue_emptyStringUsesNullValueWithOffsets() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "long").field("null_value", 7).endObject()),
            multiValueColumnarSettings(),
            batch(
                "long multi-value empty string null_value",
                1L,
                doc("d1", 1L, "{\"f\":[\"\",\"5\",\"\"]}"),
                doc("d2", 2L, "{}"),
                doc("d3", 3L, "{\"f\":[\"9\",\"\",\"1\"]}"),
                doc("d4", 4L, "{\"f\":[\"\"]}")
            )
        );
    }

    private static Settings tsdbKeepArraysSettings() {
        return Settings.builder().put(tsdbSettings()).put(Mapper.SYNTHETIC_SOURCE_KEEP_INDEX_SETTING.getKey(), "arrays").build();
    }

    private static CheckedConsumer<XContentBuilder, IOException> tsdbLongMapping() {
        return b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(FIELD).field("type", "long").endObject();
        };
    }

    /**
     * With {@code synthetic_source_keep: arrays} in time-series mode the row path records offsets only for values
     * whose immediate parent is an array, so scalar values, including a dropped empty string, stay on the
     * columnar path with no sidecar on either side.
     */
    public void testLongField_tsdbKeepArraysScalar() throws IOException {
        final String idA = TsidExtractingIdFieldMapper.createId(ST_ROUTING_HASH, ST_TSID, ST_TS_A);
        final String idB = TsidExtractingIdFieldMapper.createId(ST_ROUTING_HASH, ST_TSID, ST_TS_B);
        assertColumnarMatchesXContent(
            mapping(tsdbLongMapping()),
            tsdbKeepArraysSettings(),
            batch(
                "long tsdb keep arrays scalar",
                1L,
                doc(idA, ST_ROUTING, ST_TSID, 1L, "{\"@timestamp\":" + ST_TS_A + ",\"f\":\"42\"}"),
                doc(idB, ST_ROUTING, ST_TSID, 2L, "{\"@timestamp\":" + ST_TS_B + ",\"f\":\"\"}")
            )
        );
    }

    /**
     * As {@link #testLongField_tsdbKeepArraysScalar}, for an array value: the row path records offsets for it by a
     * rule the columnar sidecar does not reproduce outside strict-columnar modes, so the batch falls back.
     */
    public void testLongField_tsdbKeepArraysArrayBailsOut() throws IOException {
        final var mapperService = createMapperService(tsdbKeepArraysSettings(), mapping(tsdbLongMapping()));
        final UnsupportedOperationException ex = expectThrows(
            UnsupportedOperationException.class,
            () -> mapColumnarLeaf(mapperService, FIELD, "{\"@timestamp\":" + ST_TS_A + ",\"f\":[1,2]}")
        );
        assertTrue(ex.getMessage(), ex.getMessage().contains("records array offsets outside a strict-columnar index mode"));
    }

    /** TSDB settings naming both the keyword {@code dim} and the numeric {@code f} as index dimensions. */
    private static Settings tsdbDimensionSettings() {
        return Settings.builder().put(tsdbSettings()).putList(IndexMetadata.INDEX_DIMENSIONS.getKey(), "dim", FIELD).build();
    }

    public void testLongField_dimension() throws IOException {
        final String idA = TsidExtractingIdFieldMapper.createId(ST_ROUTING_HASH, ST_TSID, ST_TS_A);
        final String idB = TsidExtractingIdFieldMapper.createId(ST_ROUTING_HASH, ST_TSID, ST_TS_B);
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(FIELD).field("type", "long").field("time_series_dimension", true).endObject();
        }),
            tsdbDimensionSettings(),
            batch(
                "long dimension",
                1L,
                doc(idA, ST_ROUTING, ST_TSID, 1L, "{\"@timestamp\":" + ST_TS_A + ",\"f\":42}"),
                doc(idB, ST_ROUTING, ST_TSID, 2L, "{\"@timestamp\":" + ST_TS_B + ",\"f\":-7}")
            )
        );
    }

    public void testLongField_dimensionAbsentDoc() throws IOException {
        final String idA = TsidExtractingIdFieldMapper.createId(ST_ROUTING_HASH, ST_TSID, ST_TS_A);
        final String idB = TsidExtractingIdFieldMapper.createId(ST_ROUTING_HASH, ST_TSID, ST_TS_B);
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(FIELD).field("type", "long").field("time_series_dimension", true).endObject();
        }),
            tsdbDimensionSettings(),
            batch(
                "long dimension absent doc",
                1L,
                doc(idA, ST_ROUTING, ST_TSID, 1L, "{\"@timestamp\":" + ST_TS_A + ",\"f\":42}"),
                doc(idB, ST_ROUTING, ST_TSID, 2L, "{\"@timestamp\":" + ST_TS_B + "}")
            )
        );
    }

    public void testIntegerField_dimension() throws IOException {
        final String idA = TsidExtractingIdFieldMapper.createId(ST_ROUTING_HASH, ST_TSID, ST_TS_A);
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject(FIELD).field("type", "integer").field("time_series_dimension", true).endObject();
        }),
            tsdbDimensionSettings(),
            batch("integer dimension", 1L, doc(idA, ST_ROUTING, ST_TSID, 1L, "{\"@timestamp\":" + ST_TS_A + ",\"f\":7}"))
        );
    }

    /**
     * A {@code long} parent stringifies into its keyword sub-field on both paths. Kept to scalar values: mixing a scalar and an
     * array promotes the ESCF column to UNION, which {@code NumberFieldMapper#mapColumnBatch} does not handle yet and rejects by
     * throwing so the batch falls back — a pre-existing limitation of that mapper, unrelated to multi-fields.
     */
    public void testMultiValueViolationBailsOutOfColumnarPath() throws IOException {
        // Two values for a multi_value=false field: mapColumnBatch must throw so that
        // ShardBatchMapper falls back to the row path, which raises the correct
        // on_failure=FAIL document-level error instead.
        final var mapperService = createMapperService(
            columnarSettings(),
            mapping(b -> b.startObject(FIELD).field("type", "long").endObject())
        );
        expectThrows(UnsupportedOperationException.class, () -> mapColumnarLeaf(mapperService, FIELD, "{\"f\":[1,2]}"));
    }

    public void testNullabilityViolationBailsOutOfColumnarPath() throws IOException {
        // A null value for a nullability=false field: mapColumnBatch must throw so that
        // ShardBatchMapper falls back to the row path, which raises the correct
        // on_failure=FAIL document-level error instead.
        final var mapperService = createMapperService(columnarSettings(), mapping(b -> {
            b.startObject(FIELD).field("type", "long");
            b.startObject("doc_values").field("nullability", false).endObject();
            b.endObject();
        }));
        expectThrows(UnsupportedOperationException.class, () -> mapColumnarLeaf(mapperService, FIELD, "{\"f\":1}", "{\"f\":null}"));
    }

    public void testLongParentWithKeywordSubField() throws IOException {
        assertColumnarMatchesXContent(mapping(b -> {
            b.startObject(FIELD).field("type", "long");
            b.startObject("fields").startObject("raw").field("type", "keyword").endObject().endObject();
            b.endObject();
        }),
            columnarSettings(),
            batch(
                "long parent, keyword sub-field",
                1L,
                doc("d1", 1L, "{\"f\":42}"),
                doc("d2", 2L, "{\"f\":-7}"),
                doc("d3", 3L, "{\"f\":9876543210}"),
                doc("d4", 4L, "{}")
            )
        );
    }
}
