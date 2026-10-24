/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.logging.log4j.Level;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Parity tests for the {@code columnar_stored} source blob that {@link SourceFieldMapper#postColumnarParse} builds. The first group
 * aims at the reused blob buffer: the batch path streams every document's JSON into one buffer behind a precomputed
 * {@code SingularIgnoredSourceEncoding} header and {@code XContentDataHelper} type byte, where the row path encodes each document
 * through fresh builders and intermediate copies. The second group aims at the direct path, which writes the blob from the
 * {@link SourceColumn}s the keyword and number mappers register instead of running the synthetic source loader; those scenarios pin
 * which path the batch took through the {@link BatchMappingContext} debug log. All of them assert — through the
 * {@link AbstractColumnarMapperCompatibilityTestCase} harness, which compares the blob bytes of both paths — that the batch path
 * reproduces the row path's encoding exactly.
 */
public class SourceFieldMapperColumnarCompatibilityTests extends AbstractColumnarMapperCompatibilityTestCase {

    private static Settings columnarSettings() {
        return Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
    }

    /**
     * A mapping of only the field types that register a {@link SourceColumn}, so that the {@code columnar_stored} blob is written
     * directly. The object is flattened by the strict columnar mode, so its leaf sorts by its dotted path like every other field, and
     * the multi-field under {@code kw} must leave no trace of its own in the blob. The keyword fields take every doc values layout the
     * batch path writes: the multi-valued ones a payload per document (or an array-order blob on an index created before the ColumNAR
     * codec), the {@code multi_value: false} ones the raw value, with the {@code ignore_above} fallback next to each. {@code kw_null}
     * and {@code addr_null} substitute a {@code null_value}, and {@code lenient} carries an {@code ignore_malformed} fallback layer
     * that the batch path never fills. {@code txt} and {@code single_txt} are text fields, whose keyword multi-field must again leave no
     * trace, and {@code pt} is a geo_point, the one column group with a direct writer.
     */
    private static XContentBuilder directMapping() throws IOException {
        return mapping(b -> {
            b.startObject("kw");
            {
                b.field("type", "keyword");
                b.startObject("fields");
                b.startObject("raw").field("type", "keyword").field("ignore_above", 4).endObject();
                b.endObject();
            }
            b.endObject();
            b.startObject("single_kw").field("type", "keyword").field("multi_value", false).endObject();
            b.startObject("single_capped").field("type", "keyword").field("multi_value", false).field("ignore_above", 8).endObject();
            b.startObject("kw_null").field("type", "keyword").field("null_value", "none").endObject();
            b.startObject("txt");
            {
                b.field("type", "text");
                b.startObject("fields");
                b.startObject("keyword").field("type", "keyword").field("ignore_above", 4).endObject();
                b.endObject();
            }
            b.endObject();
            b.startObject("single_txt").field("type", "text").field("multi_value", false).endObject();
            b.startObject("pt").field("type", "geo_point").endObject();
            b.startObject("lenient").field("type", "long").field("ignore_malformed", true).endObject();
            b.startObject("num").field("type", "long").endObject();
            b.startObject("small").field("type", "integer").endObject();
            b.startObject("ratio").field("type", "double").endObject();
            b.startObject("flag").field("type", "boolean").endObject();
            b.startObject("when").field("type", "date").endObject();
            b.startObject("addr").field("type", "ip").endObject();
            b.startObject("single_addr").field("type", "ip").field("multi_value", false).endObject();
            b.startObject("addr_null").field("type", "ip").field("null_value", "0.0.0.0").endObject();
            b.startObject("capped").field("type", "keyword").field("ignore_above", 8).endObject();
            b.startObject("attrs").field("type", "flattened").endObject();
            b.startObject("attrs_null").field("type", "flattened").field("null_value", "NULL").endObject();
            b.startObject("obj");
            {
                b.startObject("properties");
                b.startObject("inner_kw").field("type", "keyword").endObject();
                b.startObject("inner_num").field("type", "long").endObject();
                b.endObject();
            }
            b.endObject();
        });
    }

    /**
     * Runs {@code scenarios} on {@link #directMapping()} and asserts that the batch never fell back to the loader path, which is the
     * only way the parity check below exercises the direct writer rather than the loader it is measured against.
     */
    private void assertDirectPath(Batch... scenarios) throws IOException {
        assertDirectPath(IndexVersion.current(), scenarios);
    }

    private void assertDirectPath(IndexVersion indexVersion, Batch... scenarios) throws IOException {
        assertColumnarStoredSourceWrittenDirectly(indexVersion, directMapping(), columnarSettings(), scenarios);
    }

    private void assertColumnarMatchesXContentUnchecked(XContentBuilder mapping, Settings indexSettings, Batch... scenarios) {
        try {
            assertColumnarMatchesXContent(mapping, indexSettings, scenarios);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Every shape the source layers have to reproduce: scalars, arrays, single-element arrays (a scalar for numbers, whose offsets
     * only record two slots or more), empty arrays (absent for numbers, {@code []} for array-order ip), nulls alone and inside arrays,
     * all-null arrays, duplicates, an over-long keyword that lands in the {@code ignore_above} fallback next to short ones that stay in
     * the payload, booleans and dates as literals and as strings, single-valued keywords with and without a fallback, nulls that a
     * {@code null_value} replaces alone and inside arrays, and documents that leave fields out entirely. The documents are
     * interleaved with empty ones so each layer has to skip documents correctly. Numbers keep one shape and one width per batch: a
     * column that mixes a scalar with an array, or narrow with wide literals, becomes a {@code UNION} the number mapper refuses,
     * which is a limit of the batch path itself and not of the source writer; booleans and dates take scalars only for the same reason.
     */
    public void testDirectPathWritesEveryValueShape() throws IOException {
        assertDirectPath(everyValueShape());
    }

    /**
     * The same documents on an index created before the ColumNAR codec, where a multi-valued keyword keeps its array order in an
     * {@code ArrayOrderInlineNull} blob next to a counts column and a single-valued one is a plain binary doc value: the direct
     * writer has to read those layouts back the way their loader layers do.
     */
    public void testDirectPathWritesEveryValueShapeBeforeColumnarCodec() throws IOException {
        assertDirectPath(IndexVersionUtils.getPreviousVersion(IndexVersions.COLUMNAR_CODEC_NO_FEATURE_FLAG), everyValueShape());
    }

    private static Batch[] everyValueShape() {
        return new Batch[] {
            batch(
                "scalar numbers",
                1L,
                doc(
                    "d1",
                    1L,
                    "{\"kw\":\"one\",\"num\":1,\"small\":-3,\"ratio\":0.25,\"capped\":\"short\",\"lenient\":42,"
                        + "\"single_kw\":\"solo\",\"single_capped\":\"waytoolongforthelimit\",\"kw_null\":null,\"addr_null\":null,"
                        + "\"txt\":\"some words here\",\"single_txt\":\"one line\",\"pt\":{\"lat\":41.12,\"lon\":-71.34},"
                        + "\"attrs\":{\"z\":\"last\",\"a\":{\"b\":[\"two\",\"one\",\"two\"]},\"n\":1.5,\"t\":true},"
                        + "\"attrs_null\":{\"k\":null},\"obj\":{\"inner_kw\":\"x\",\"inner_num\":9}}"
                ),
                doc("d2", 2L, "{}"),
                doc(
                    "d3",
                    3L,
                    "{\"kw\":[\"b\",\"a\",\"b\"],\"num\":7,\"capped\":[\"short\",\"waytoolongforthelimit\",\"tiny\"],"
                        + "\"flag\":true,\"when\":\"2026-10-17T12:00:00Z\",\"addr\":\"10.0.0.1\",\"single_addr\":\"2001:db8::1\","
                        + "\"single_kw\":[\"one\"],\"single_capped\":\"short\",\"kw_null\":[null,\"a\",null],"
                        + "\"addr_null\":[\"10.0.0.1\",null],\"txt\":[\"b b\",\"a\",null,\"b b\"],\"single_txt\":[\"x\"],"
                        + "\"pt\":{\"lat\":-90.0,\"lon\":180.0},\"attrs\":{\"a\":{\"b\":\"nested\"},\"a.b\":\"dotted\",\"c\":[null]},"
                        + "\"attrs_null\":{\"k\":[null,\"x\"],\"m\":{\"n\":\"\"}}}"
                ),
                doc(
                    "d4",
                    4L,
                    "{\"kw\":null,\"capped\":[null,\"v\"],\"obj\":{\"inner_num\":2},\"flag\":false,"
                        + "\"addr\":[\"10.0.0.2\",\"10.0.0.1\",\"10.0.0.2\"]}"
                ),
                doc(
                    "d5",
                    5L,
                    "{\"capped\":\"waytoolongforthelimit\",\"small\":0,\"ratio\":-1.5e300,\"when\":\"2026-01-01\",\"addr\":[],"
                        + "\"single_kw\":null,\"single_capped\":[],\"kw_null\":\"b\",\"addr_null\":[null,null],\"attrs\":{}}"
                ),
                doc("d6", 6L, "{\"obj\":{\"inner_kw\":[\"z\",\"y\"]},\"addr\":[null,\"::1\"],\"single_addr\":null}"),
                doc(
                    "d7",
                    7L,
                    "{\"addr\":[null,null],\"single_kw\":[],\"single_capped\":[\"waytoolongforthelimit\"],\"kw_null\":[],"
                        + "\"txt\":[null,null],\"single_txt\":null,\"pt\":null,\"attrs\":null,\"attrs_null\":{\"k\":[]}}"
                ),
                doc(
                    "d18",
                    18L,
                    "{\"single_capped\":null,\"kw_null\":[\"a\",\"a\"],\"addr_null\":\"::1\",\"lenient\":-7,\"txt\":[],"
                        + "\"single_txt\":[],\"pt\":{\"lat\":0.5,\"lon\":0.25},\"attrs\":{\"k1\":null,\"k2\":null},"
                        + "\"attrs_null\":{\"a\":{\"b\":{\"c\":\"deep\"}},\"a.b.d\":7}}"
                )
            ),
            batch(
                "booleans and dates as strings and numbers",
                1L,
                doc("d14", 14L, "{\"flag\":\"true\",\"when\":1760702400000}"),
                doc("d15", 15L, "{\"flag\":\"false\",\"when\":1704067200000}"),
                doc("d16", 16L, "{\"addr\":[\"::ffff:10.0.0.1\"],\"single_addr\":\"::ffff:10.0.0.1\"}")
            ),
            batch(
                "number arrays",
                1L,
                doc("d8", 7L, "{\"kw\":[\"solo\"],\"num\":[5],\"small\":[7]}"),
                doc("d9", 8L, "{\"kw\":[],\"num\":[],\"capped\":[]}"),
                doc("d10", 9L, "{\"num\":[3,1,3,2],\"small\":[-1,-1],\"obj\":{\"inner_num\":[2,2]}}"),
                doc("d11", 10L, "{}"),
                doc("d12", 11L, "{\"num\":[4,4,4],\"obj\":{\"inner_num\":[1]}}")
            ),
            batch(
                "wide numbers",
                1L,
                doc("d13", 12L, "{\"num\":[9223372036854775807,-9223372036854775808]}"),
                doc("d17", 13L, "{\"num\":[-9223372036854775808]}")
            ) };
    }

    /**
     * The direct writer orders fields by their full path, as the loader does, which is not the order they appear in the document nor
     * the order the mappers ran in.
     */
    public void testDirectPathOrdersFieldsByPath() throws IOException {
        assertDirectPath(
            batch(
                "field order",
                1L,
                doc(
                    "d1",
                    1L,
                    "{\"when\":\"2026-05-05\",\"small\":1,\"obj\":{\"inner_num\":2,\"inner_kw\":\"c\"},\"single_addr\":\"::1\","
                        + "\"num\":3,\"flag\":true,\"kw\":\"a\",\"attrs\":{\"y\":\"1\",\"x\":\"2\"},\"addr\":\"::2\",\"capped\":\"b\"}"
                ),
                doc("d2", 2L, "{\"obj\":{\"inner_kw\":\"c\"},\"capped\":\"b\",\"addr\":[\"::3\"]}")
            )
        );
    }

    /**
     * The direct writer must produce the same bytes as the loader for any mix of documents, not only the hand-picked ones above. The
     * generator stays within what the batch path accepts, as the harness refuses anything else: no nulls or malformed values among
     * numbers, and per batch one shape (scalar or array) and one literal width per number field, see
     * {@link #testDirectPathWritesEveryValueShape}.
     */
    public void testDirectPathMatchesLoaderOnRandomDocuments() throws IOException {
        assertDirectPath(randomBatches());
    }

    /** {@link #testDirectPathMatchesLoaderOnRandomDocuments} on an index created before the ColumNAR codec. */
    public void testDirectPathMatchesLoaderOnRandomDocumentsBeforeColumnarCodec() throws IOException {
        assertDirectPath(IndexVersionUtils.getPreviousVersion(IndexVersions.COLUMNAR_CODEC_NO_FEATURE_FLAG), randomBatches());
    }

    private static Batch[] randomBatches() {
        final Batch[] batches = new Batch[randomIntBetween(1, 3)];
        long seqNo = 1;
        for (int b = 0; b < batches.length; b++) {
            final NumberShape num = NumberShape.random();
            final NumberShape small = NumberShape.random();
            final NumberShape innerNum = NumberShape.random();
            final NumberShape lenient = NumberShape.random();
            final int docCount = randomIntBetween(1, 30);
            final Doc[] docs = new Doc[docCount];
            for (int d = 0; d < docCount; d++, seqNo++) {
                docs[d] = doc("d" + seqNo, seqNo, randomDocument(num, small, innerNum, lenient));
            }
            batches[b] = batch("random " + b, 1L, docs);
        }
        return batches;
    }

    /** The shape and literal width every value of a number field keeps within one batch. */
    private record NumberShape(boolean arrays, boolean wide) {
        static NumberShape random() {
            return new NumberShape(randomBoolean(), randomBoolean());
        }

        /**
         * A scalar or an array of one to five values that may repeat. Never a null, which the number mapper does not take in batch,
         * and never an empty array: a batch whose only array for a field is empty gives the column no element kind, which the mapper
         * refuses as a {@code UNION}. {@link #testDirectPathWritesEveryValueShape} covers empty arrays next to filled ones.
         */
        String randomValue() {
            final Supplier<String> value = wide
                ? () -> Long.toString(
                    randomBoolean() ? randomLongBetween(1L << 40, Long.MAX_VALUE) : randomLongBetween(Long.MIN_VALUE, -(1L << 40))
                )
                : () -> Integer.toString(randomIntBetween(-100, 100));
            return arrays ? randomArray(value, 1) : value.get();
        }
    }

    private static String randomDocument(NumberShape num, NumberShape small, NumberShape innerNum, NumberShape lenient) {
        final List<String> fields = new ArrayList<>();
        if (randomBoolean()) {
            fields.add("\"kw\":" + randomKeywordValue(() -> "\"" + randomAlphaOfLengthBetween(0, 12) + "\""));
        }
        if (randomBoolean()) {
            fields.add("\"capped\":" + randomKeywordValue(() -> "\"" + randomAlphaOfLengthBetween(0, 12) + "\""));
        }
        if (randomBoolean()) {
            fields.add("\"kw_null\":" + randomKeywordValue(() -> "\"" + randomAlphaOfLengthBetween(0, 12) + "\""));
        }
        if (randomBoolean()) {
            fields.add("\"single_kw\":" + randomSingleValue(() -> "\"" + randomAlphaOfLengthBetween(0, 12) + "\""));
        }
        if (randomBoolean()) {
            fields.add("\"single_capped\":" + randomSingleValue(() -> "\"" + randomAlphaOfLengthBetween(0, 12) + "\""));
        }
        if (randomBoolean()) {
            fields.add(
                "\"txt\":" + randomKeywordValue(() -> "\"" + randomAlphaOfLengthBetween(0, 12) + " " + randomAlphaOfLength(3) + "\"")
            );
        }
        if (randomBoolean()) {
            fields.add("\"single_txt\":" + randomSingleValue(() -> "\"" + randomAlphaOfLengthBetween(0, 12) + "\""));
        }
        if (randomBoolean()) {
            // The object form only: the batch path refuses the string and array forms, and null stands for an absent point.
            fields.add(
                "\"pt\":"
                    + (rarely()
                        ? "null"
                        : "{\"lat\":" + randomDoubleBetween(-90, 90, true) + ",\"lon\":" + randomDoubleBetween(-180, 180, true) + "}")
            );
        }
        if (randomBoolean()) {
            fields.add("\"attrs\":" + randomFlattenedValue());
        }
        if (randomBoolean()) {
            fields.add("\"attrs_null\":" + randomFlattenedValue());
        }
        if (randomBoolean()) {
            fields.add("\"num\":" + num.randomValue());
        }
        if (randomBoolean()) {
            fields.add("\"lenient\":" + lenient.randomValue());
        }
        if (randomBoolean() && small.wide() == false) {
            fields.add("\"small\":" + small.randomValue());
        }
        if (randomBoolean()) {
            // Scalars only: double literals of different widths in one column make a UNION under the SIMD encoder.
            fields.add("\"ratio\":" + randomDouble());
        }
        if (randomBoolean()) {
            // Scalars only, and one literal kind per batch would be needed to mix booleans with their string forms; the boolean
            // mapper refuses arrays and the UNION a mix makes.
            fields.add("\"flag\":" + randomBoolean());
        }
        if (randomBoolean()) {
            fields.add(
                "\"when\":\""
                    + DateFormatter.forPattern("strict_date_optional_time").formatMillis(randomLongBetween(0, 4102444800000L))
                    + "\""
            );
        }
        if (randomBoolean()) {
            fields.add("\"addr\":" + randomKeywordValue(SourceFieldMapperColumnarCompatibilityTests::randomIpLiteral));
        }
        if (randomBoolean()) {
            fields.add("\"single_addr\":" + randomSingleValue(SourceFieldMapperColumnarCompatibilityTests::randomIpLiteral));
        }
        if (randomBoolean()) {
            fields.add("\"addr_null\":" + randomKeywordValue(SourceFieldMapperColumnarCompatibilityTests::randomIpLiteral));
        }
        final List<String> inner = new ArrayList<>();
        if (randomBoolean()) {
            inner.add("\"inner_kw\":" + randomKeywordValue(() -> "\"" + randomAlphaOfLengthBetween(1, 4) + "\""));
        }
        if (randomBoolean()) {
            inner.add("\"inner_num\":" + innerNum.randomValue());
        }
        if (inner.isEmpty() == false) {
            // An empty object would be a leaf of its own in the schema, with no mapper at its path once the object is flattened.
            fields.add("\"obj\":{" + String.join(",", inner) + "}");
        }
        return "{" + String.join(",", fields) + "}";
    }

    private static String randomIpLiteral() {
        return "\"" + NetworkAddress.format(randomIp(randomBoolean())) + "\"";
    }

    /** A scalar, a null, or an array of up to five values that may repeat and may hold nulls. */
    private static String randomKeywordValue(Supplier<String> value) {
        return switch (randomIntBetween(0, 3)) {
            case 0 -> value.get();
            case 1 -> "null";
            default -> randomArray(() -> rarely() ? "null" : value.get(), 0);
        };
    }

    /** What a {@code multi_value: false} field takes: a scalar, a null, an empty array, or an array of one value or null. */
    private static String randomSingleValue(Supplier<String> value) {
        return switch (randomIntBetween(0, 4)) {
            case 0, 1 -> value.get();
            case 2 -> "null";
            case 3 -> "[]";
            default -> "[" + (rarely() ? "null" : value.get()) + "]";
        };
    }

    private static String randomArray(Supplier<String> element, int minSize) {
        final int size = randomIntBetween(minSize, 5);
        final List<String> elements = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            // Repeat the previous element now and then so that duplicates show up next to each other as well as apart.
            elements.add(i > 0 && rarely() ? elements.get(i - 1) : element.get());
        }
        return "[" + String.join(",", elements) + "]";
    }

    /**
     * The keys of a random flattened value, in the one order every document lists them in. The batch path writes the slots of a
     * document in the order the batch schema first saw their keys, not in the document's order (see
     * {@link org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper#mapColumnGroupBatch}), so a document only takes a prefix
     * of this list, which keeps the two orders equal and the {@code _keyed} blobs of both paths byte-identical. {@code a} comes last so
     * that it meets {@code a.b} as a scalar under an object only in the widest documents.
     */
    private static final String[] FLATTENED_KEYS = { "a.b", "a.c", "b", "k", "a" };

    /**
     * A flattened value the batch path accepts: null, an empty object, or an object of the first few {@link #FLATTENED_KEYS}, whose
     * leaves are strings, numbers, booleans, nulls, or arrays of those. The keys are always spelled dotted: the nested spelling makes
     * a column of its own in the batch schema, so mixing the two across a batch would reorder the blob as well. The nested spelling,
     * and a scalar next to an object under one prefix, are covered by {@link #everyValueShape()}.
     */
    private static String randomFlattenedValue() {
        if (rarely()) {
            return randomBoolean() ? "null" : "{}";
        }
        final Supplier<String> leaf = () -> switch (randomIntBetween(0, 4)) {
            case 0 -> "null";
            case 1 -> Long.toString(randomLong());
            case 2 -> Boolean.toString(randomBoolean());
            case 3 -> "\"" + randomAlphaOfLengthBetween(0, 8) + "\"";
            case 4 -> "\"" + randomFrom("ключ", "值", "café", "ünïcøde") + "\"";
            default -> throw new AssertionError();
        };
        final int keyCount = randomIntBetween(1, FLATTENED_KEYS.length);
        final List<String> entries = new ArrayList<>();
        for (int i = 0; i < keyCount; i++) {
            entries.add("\"" + FLATTENED_KEYS[i] + "\":" + (randomBoolean() ? leaf.get() : randomArray(leaf, 0)));
        }
        return "{" + String.join(",", entries) + "}";
    }

    /**
     * A field without a direct source writer sends the whole batch down the loader path, and the blob must still match. Every field
     * type the batch path takes now has one, so the field here is the {@code _unmapped} sink, which absorbs unmapped keys and renders
     * them at the root rather than under its own name, a shape no {@link SourceColumn} describes. The same documents, plus one
     * unmapped key, are written by the loader this time; the debug log names the field that caused it.
     */
    @TestLogging(reason = "the direct path reports a fallback at DEBUG", value = "org.elasticsearch.index.mapper.BatchMappingContext:DEBUG")
    public void testUnsupportedFieldTakesTheLoaderPath() throws IOException {
        assumeTrue("the _unmapped sink is behind a feature flag", FlattenedFieldMapper.UNMAPPED_FIELDS_FEATURE_FLAG.isEnabled());
        final XContentBuilder mappingWithSink = mapping(b -> {
            b.startObject("kw").field("type", "keyword").endObject();
            b.startObject("num").field("type", "long").endObject();
        });
        final Settings settingsWithSink = Settings.builder()
            .put(columnarSettings())
            .put(IndexSettings.FLATTENED_UNMAPPED_FIELDS_ENABLED.getKey(), true)
            .build();
        MockLog.assertThatLogger(
            () -> assertColumnarMatchesXContentUnchecked(
                mappingWithSink,
                settingsWithSink,
                batch(
                    "unmapped key present",
                    1L,
                    doc("d1", 1L, "{\"kw\":\"one\",\"num\":[1,2],\"extra\":{\"a\":\"x\",\"b\":[\"y\",\"z\"]}}"),
                    doc("d2", 2L, "{\"kw\":[\"a\",\"b\"]}")
                )
            ),
            BatchMappingContext.class,
            new MockLog.SeenEventExpectation(
                "loader path taken",
                BatchMappingContext.class.getCanonicalName(),
                Level.DEBUG,
                "*takes the loader path: field [_unmapped] of type [_unmapped] has no direct source writer"
            )
        );
    }

    private static XContentBuilder testMapping() throws IOException {
        return mapping(b -> {
            b.startObject("kw").field("type", "keyword").endObject();
            b.startObject("num").field("type", "long").endObject();
            b.startObject("date").field("type", "date").endObject();
            b.startObject("text");
            {
                b.field("type", "text");
                b.startObject("fields");
                b.startObject("keyword").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
            b.startObject("capped").field("type", "keyword").field("ignore_above", 8).endObject();
            b.startObject("obj");
            {
                b.startObject("properties");
                b.startObject("inner_kw").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
        });
    }

    /**
     * Documents ordered from large to small, ending in an empty one: the batch path writes each blob over the previous one in the
     * same buffer, so any stale byte past the reset point — the bug class a reused, in-place-exposed buffer invites — would surface
     * as a longer blob than the row path produces.
     */
    public void testShrinkingDocumentsReuseTheBlobBuffer() throws IOException {
        final String big = "x".repeat(500);
        assertColumnarMatchesXContent(
            testMapping(),
            columnarSettings(),
            batch(
                "shrinking docs",
                1L,
                doc("d1", 1L, "{\"kw\":\"" + big + "\",\"text\":\"" + big + " " + big + "\",\"num\":123456789012345}"),
                doc("d2", 2L, "{\"kw\":[\"alpha\",\"beta\"],\"obj\":{\"inner_kw\":\"inner\"}}"),
                doc("d3", 3L, "{\"num\":7}"),
                doc("d4", 4L, "{}")
            )
        );
    }

    /**
     * Growing and shrinking across consecutive documents, with values whose encoded forms differ in kind (dates, multi-values,
     * the {@code ignore_above} fallback that embeds its own {@code _ignored_source} entry inside the blob): every document must
     * come out byte-identical to the row path's fresh-builder encoding regardless of what the previous document left in the buffer.
     */
    public void testAlternatingDocumentSizes() throws IOException {
        final String big = "y".repeat(300);
        assertColumnarMatchesXContent(
            testMapping(),
            columnarSettings(),
            batch(
                "alternating sizes",
                1L,
                doc("d1", 1L, "{\"num\":1}"),
                doc("d2", 2L, "{\"kw\":\"" + big + "\",\"date\":\"2026-10-17T12:00:00Z\"}"),
                doc("d3", 3L, "{\"capped\":\"waytoolongforthelimit\"}"),
                doc("d4", 4L, "{\"text\":\"" + big + "\",\"kw\":[\"a\",\"b\",\"c\"],\"capped\":\"short\"}"),
                doc("d5", 5L, "{\"date\":\"2026-01-01\"}")
            )
        );
    }

    /**
     * Several batches against the same mapper service: the buffer and the prefix are per batch, so each batch must stand on its
     * own, including a batch whose first document is empty (nothing but the prefix in the buffer).
     */
    public void testBufferIsPerBatch() throws IOException {
        assertColumnarMatchesXContent(
            testMapping(),
            columnarSettings(),
            batch("first", 1L, doc("d1", 1L, "{\"kw\":\"one\",\"num\":1}")),
            batch("empty leading doc", 1L, doc("d2", 2L, "{}"), doc("d3", 3L, "{\"kw\":\"two\"}")),
            batch("wider", 1L, doc("d4", 4L, "{\"kw\":\"three\",\"date\":\"2026-05-05\",\"obj\":{\"inner_kw\":\"x\"}}"))
        );
    }
}
