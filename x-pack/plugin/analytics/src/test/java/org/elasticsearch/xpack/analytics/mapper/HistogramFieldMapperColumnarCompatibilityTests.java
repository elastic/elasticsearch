/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.analytics.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.AbstractColumnarMapperCompatibilityTestCase;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.TimeSeriesRoutingHashFieldMapper;
import org.elasticsearch.index.mapper.TsidExtractingIdFieldMapper;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

/**
 * Parity tests for {@link HistogramFieldMapper#mapColumnGroupBatch} against the row path.
 * <p>
 * The {@link AbstractColumnarMapperCompatibilityTestCase} harness drives the group mapper through
 * {@link org.elasticsearch.index.mapper.ColumnGroupResolver}: it recognises the {@code h.values} and
 * {@code h.counts} leaves the encoder produces for a histogram object as owned by the {@code histogram}
 * group mapper, and calls {@code mapColumnGroupBatch} with the paired columns.
 * <p>
 * Each test runs in one index mode that takes the batch path, picked at random.
 */
public class HistogramFieldMapperColumnarCompatibilityTests extends AbstractColumnarMapperCompatibilityTestCase {

    private static final String FIELD = "h";

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new AnalyticsPlugin());
    }

    private static final BytesRef TSID = new BytesRef(new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05 });
    private static final int ROUTING_HASH = 42;
    private static final String ROUTING = TimeSeriesRoutingHashFieldMapper.encode(ROUTING_HASH);
    // epoch millis: 2024-01-15T12:00:00.000Z
    private static final long TIMESTAMP = 1705320000000L;

    private IndexMode indexMode;

    @Before
    public void pickIndexMode() {
        indexMode = randomFrom(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR, IndexMode.VECTORDB_COLUMNAR, IndexMode.TIME_SERIES);
    }

    /** Settings for an index in {@link #indexMode}, one of the modes whose indices take the batch path. */
    private Settings escfEligibleSettings() {
        final Settings.Builder settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), indexMode.getName())
            .put(RecoverySettings.INDICES_RECOVERY_SOURCE_ENABLED_SETTING.getKey(), false);
        if (indexMode.isTsdb()) {
            settings.put(IndexMetadata.INDEX_DIMENSIONS.getKey(), "dim")
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "-9999-01-01T00:00:00Z")
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "9999-01-01T00:00:00Z")
                .put(IndexSettings.SYNTHETIC_ID.getKey(), false);
        }
        return settings.build();
    }

    private XContentBuilder histogramMapping() throws IOException {
        return histogramMapping(b -> {});
    }

    /**
     * Maps {@code h} as a histogram with {@code options}. A time-series index also gets the {@code @timestamp} and dimension
     * fields it requires; the dimension is left out of the documents, which carry the coordinator's tsid instead.
     */
    private XContentBuilder histogramMapping(CheckedConsumer<XContentBuilder, IOException> options) throws IOException {
        return mapping(b -> {
            if (indexMode.isTsdb()) {
                b.startObject("@timestamp").field("type", "date").endObject();
                b.startObject("dim").field("type", "keyword").field("time_series_dimension", true).endObject();
            }
            b.startObject(FIELD).field("type", "histogram");
            if (indexMode.isTsdb() && randomBoolean()) {
                b.field("time_series_metric", "histogram");
            }
            options.accept(b);
            b.endObject();
        });
    }

    /**
     * A document with the given JSON {@code source}, plus the {@code @timestamp} that time-series and logsdb indices
     * demand. A time-series document also gets the {@code _id} and routing the coordinator derives from the timestamp and
     * the tsid; the timestamp varies with {@code seqNo} to keep those {@code _id}s distinct.
     */
    private Doc histogramDoc(long seqNo, String source) {
        final long timestamp = TIMESTAMP + seqNo;
        return switch (indexMode) {
            case COLUMNAR, VECTORDB_COLUMNAR -> doc("d" + seqNo, seqNo, source);
            case LOGSDB_COLUMNAR -> doc("d" + seqNo, seqNo, withTimestamp(source, timestamp));
            case TIME_SERIES -> doc(
                TsidExtractingIdFieldMapper.createId(ROUTING_HASH, TSID, timestamp),
                ROUTING,
                TSID,
                seqNo,
                withTimestamp(source, timestamp)
            );
            default -> throw new AssertionError("not an index mode the batch path runs in: " + indexMode);
        };
    }

    private static String withTimestamp(String source, long timestamp) {
        final String fields = source.substring(1);
        return "{\"@timestamp\":" + timestamp + (fields.equals("}") ? "" : ",") + fields;
    }

    public void testMultipleDocs() throws IOException {
        assertColumnarMatchesXContent(
            histogramMapping(),
            escfEligibleSettings(),
            batch(
                "multiple docs",
                1L,
                histogramDoc(1L, "{\"h\":{\"values\":[1.0,2.5],\"counts\":[3,8]}}"),
                histogramDoc(2L, "{\"h\":{\"values\":[0.5,1.5,2.5],\"counts\":[1,1,1]}}"),
                histogramDoc(3L, "{\"h\":{\"values\":[100.0],\"counts\":[42]}}")
            )
        );
    }

    /**
     * {@code counts} written before {@code values}: the mapper keys off the relative key, not leaf order.
     */
    public void testKeysInEitherOrder() throws IOException {
        assertColumnarMatchesXContent(
            histogramMapping(),
            escfEligibleSettings(),
            batch(
                "keys in either order",
                1L,
                histogramDoc(1L, "{\"h\":{\"counts\":[3,8],\"values\":[1.0,2.5]}}"),
                histogramDoc(2L, "{\"h\":{\"values\":[1.0,2.5],\"counts\":[3,8]}}")
            )
        );
    }

    /**
     * Values given as JSON integers, so the {@code values} column's elements are LONG rather than DOUBLE.
     * The mapper accepts both, widening to the double the T-Digest encoding stores.
     */
    public void testIntegerValues() throws IOException {
        assertColumnarMatchesXContent(
            histogramMapping(),
            escfEligibleSettings(),
            batch(
                "integer values",
                1L,
                histogramDoc(1L, "{\"h\":{\"values\":[1,2,7],\"counts\":[3,8,1]}}"),
                histogramDoc(2L, "{\"h\":{\"values\":[4,5],\"counts\":[1,1]}}")
            )
        );
    }

    /**
     * {@link org.elasticsearch.xpack.core.analytics.mapper.EncodedTDigest} drops zero-count centroids, so a
     * histogram whose counts are all zero encodes to an empty value rather than no value at all.
     */
    public void testZeroCounts() throws IOException {
        assertColumnarMatchesXContent(
            histogramMapping(),
            escfEligibleSettings(),
            batch(
                "zero counts",
                1L,
                histogramDoc(1L, "{\"h\":{\"values\":[1.0,2.0,3.0],\"counts\":[0,4,0]}}"),
                histogramDoc(2L, "{\"h\":{\"values\":[1.0,2.0],\"counts\":[0,0]}}")
            )
        );
    }

    /**
     * An empty histogram alongside real ones. Its arrays are present-with-no-elements rather than absent, so it
     * must still get the empty binary value rather than be skipped.
     */
    public void testEmptyArraysMixedWithValues() throws IOException {
        assertColumnarMatchesXContent(
            histogramMapping(),
            escfEligibleSettings(),
            batch(
                "empty arrays mixed with values",
                1L,
                histogramDoc(1L, "{\"h\":{\"values\":[1.0,2.0],\"counts\":[1,2]}}"),
                histogramDoc(2L, "{\"h\":{\"values\":[],\"counts\":[]}}"),
                histogramDoc(3L, "{}"),
                histogramDoc(4L, "{\"h\":{\"values\":[3.0],\"counts\":[3]}}")
            )
        );
    }

    /** Repeated means are allowed: {@code HistogramParser} rejects decreasing, not equal, values. */
    public void testRepeatedValues() throws IOException {
        assertColumnarMatchesXContent(
            histogramMapping(),
            escfEligibleSettings(),
            batch("repeated values", 1L, histogramDoc(1L, "{\"h\":{\"values\":[2.0,2.0,2.0],\"counts\":[1,2,3]}}"))
        );
    }

    public void testNegativeValues() throws IOException {
        assertColumnarMatchesXContent(
            histogramMapping(),
            escfEligibleSettings(),
            batch("negative values", 1L, histogramDoc(1L, "{\"h\":{\"values\":[-7.5,-1.0,0.0,3.0],\"counts\":[1,2,3,4]}}"))
        );
    }

    /**
     * A null histogram produces a leaf at the field's own path rather than the group's leaves, and indexes
     * nothing on either path.
     */
    public void testNullAtOwnPath() throws IOException {
        assertColumnarMatchesXContent(
            histogramMapping(),
            escfEligibleSettings(),
            batch("null at own path", 1L, histogramDoc(1L, "{\"h\":null}"), histogramDoc(2L, "{}"), histogramDoc(3L, "{\"h\":null}"))
        );
    }

    /**
     * A batch mixing a null own-path leaf with the object form: the harness routes the null leaf to
     * {@code doMapColumnBatch} and the sub-leaves to {@code mapColumnGroupBatch}.
     */
    public void testMixedNullAndObjectForm() throws IOException {
        assertColumnarMatchesXContent(
            histogramMapping(),
            escfEligibleSettings(),
            batch(
                "mixed null and object form",
                1L,
                histogramDoc(1L, "{\"h\":null}"),
                histogramDoc(2L, "{\"h\":{\"values\":[1.0,2.5],\"counts\":[3,8]}}"),
                histogramDoc(3L, "{}")
            )
        );
    }

    public void testLargerMixedBatch() throws IOException {
        assertColumnarMatchesXContent(
            histogramMapping(),
            escfEligibleSettings(),
            batch(
                "larger mixed batch",
                2L,  // non-trivial primary term
                histogramDoc(1L, "{\"h\":{\"values\":[1.0,2.0],\"counts\":[1,2]}}"),
                histogramDoc(2L, "{}"),
                histogramDoc(3L, "{\"h\":null}"),
                histogramDoc(4L, "{\"h\":{\"values\":[3.0],\"counts\":[0]}}"),
                histogramDoc(5L, "{\"h\":{\"counts\":[7,7,7],\"values\":[1.5,2.5,3.5]}}"),
                histogramDoc(6L, "{}"),
                histogramDoc(7L, "{\"h\":{\"values\":[9.0],\"counts\":[1]}}")
            )
        );
    }

    /**
     * The encoder tags array elements with a width-specific type byte and demands they all agree before building
     * a columnar array, so mixing {@code int}- and {@code long}-ranged numbers, or an integer with a fractional
     * number, gives a UNION column of inline arrays. Both are ordinary histograms, so both must still index.
     */
    public void testMixedWidthNumbersInOneArray() throws IOException {
        assertColumnarMatchesXContent(
            histogramMapping(),
            escfEligibleSettings(),
            batch(
                "mixed width numbers in one array",
                1L,
                histogramDoc(1L, "{\"h\":{\"values\":[1.0,2.0],\"counts\":[9007199254740993,1]}}"),
                histogramDoc(2L, "{\"h\":{\"values\":[1,2.5],\"counts\":[1,1]}}")
            )
        );
    }

    /**
     * {@code HistogramParser} reads counts with the default coerce policy, which truncates a fractional number
     * toward zero rather than rejecting it, so {@code 2.5} indexes as {@code 2}. The batch path has to truncate
     * identically — the comparison here is over the encoded T-Digest bytes, so an off-by-one count would fail.
     * <p>
     * {@code -0.5} is the case worth pinning: truncation happens before the non-negative check, so both paths
     * accept it as {@code 0} where a negative integer would be rejected outright.
     */
    public void testFractionalCountsAreTruncated() throws IOException {
        assertColumnarMatchesXContent(
            histogramMapping(),
            escfEligibleSettings(),
            batch(
                "fractional counts",
                1L,
                histogramDoc(1L, "{\"h\":{\"values\":[1.0,2.0],\"counts\":[2.5,3.999]}}"),
                histogramDoc(2L, "{\"h\":{\"values\":[1.0],\"counts\":[-0.5]}}"),
                histogramDoc(3L, "{\"h\":{\"values\":[1.0,2.0],\"counts\":[1,2.5]}}"),
                histogramDoc(4L, "{\"h\":{\"values\":[5.0],\"counts\":[7]}}")
            )
        );
    }

    /**
     * Arrays that are empty in every document never resolve an element kind, so the leaves become UNION columns
     * of zero-length arrays. An empty histogram is well-formed, so these must still index.
     */
    public void testAllEmptyArrays() throws IOException {
        assertColumnarMatchesXContent(
            histogramMapping(),
            escfEligibleSettings(),
            batch(
                "all empty arrays",
                1L,
                histogramDoc(1L, "{\"h\":{\"values\":[],\"counts\":[]}}"),
                histogramDoc(2L, "{\"h\":{\"values\":[],\"counts\":[]}}")
            )
        );
    }

    // -------------------------------------------------------------------------
    // Bail-out tests — shapes the batch path must refuse so the row path handles them.
    // -------------------------------------------------------------------------

    public void testOwnPathObjectFormsFallBack() throws IOException {
        // An empty object, a scalar, and an array of histogram objects all produce a single leaf at the
        // field's own path that is not a JSON null.
        for (String source : List.of(
            "{\"h\":{}}",
            "{\"h\":\"not a histogram\"}",
            "{\"h\":42}",
            "{\"h\":[{\"values\":[1.0],\"counts\":[1]},{\"values\":[2.0],\"counts\":[2]}]}"
        )) {
            assertFallsBack(histogramMapping(), source);
        }
    }

    /** An empty object beside a full histogram: the group leaves are fine, so the own-path leaf has to refuse the batch. */
    public void testEmptyObjectMixedWithObjectFormFallsBack() throws IOException {
        final UnsupportedOperationException e = assertFallsBack(
            histogramMapping(),
            "{\"h\":{\"values\":[1.0],\"counts\":[1]}}",
            "{\"h\":{}}"
        );
        assertThat(e.getMessage(), containsString("at own path uses an unsupported value form"));
    }

    /**
     * Shapes that reach {@code mapColumnGroupBatch} and are refused there.
     * <p>
     * Each case names the check it must trip: the column-kind checks run before the per-row and per-element
     * ones, so asserting only that <em>something</em> threw would let them mask every later check.
     */
    public void testGroupBailOuts() throws IOException {
        // Key dispatch.
        assertGroupBailOut("{\"h\":{\"values\":[1.0],\"counts\":[1],\"extra\":3}}", "unexpected relative key [extra]");
        assertGroupBailOut("{\"h\":{\"values\":[1.0]}}", "is missing [counts]");
        assertGroupBailOut("{\"h\":{\"counts\":[1]}}", "is missing [values]");

        // Failures of the array iterators, which are covered in full by their own tests.
        assertGroupBailOut("{\"h\":{\"values\":1.0,\"counts\":1}}", "[values] value in document [0] is [DOUBLE], not an array");
        assertGroupBailOut("{\"h\":{\"values\":[1.0],\"counts\":[1e30]}}", "[counts] element [1.0E30] is out of range for a long");
        // HistogramParser demands an array under each key, so null fails rather than reading as empty.
        assertGroupBailOut("{\"h\":{\"values\":null,\"counts\":null}}", "[values] value in document [0] is [NULL], not an array");

        // Per-element validation inside the centroid iterator.
        assertGroupBailOut("{\"h\":{\"values\":[2.0,1.0],\"counts\":[1,1]}}", "[values] values are not in increasing order");
        assertGroupBailOut("{\"h\":{\"values\":[1.0,2.0],\"counts\":[1,-1]}}", "[counts] has a negative element");
        assertGroupBailOut("{\"h\":{\"values\":[1.0,2.0],\"counts\":[1]}}", "have different lengths");
    }

    /**
     * Spelling the same key both nested and dotted gives the group two columns with one relative key.
     * {@code ShardBatchMapper#resolveMappers} rejects that aliasing only for per-leaf mappers, so it does reach
     * a group mapper, and a histogram has nowhere to put a second {@code values} array.
     */
    public void testDuplicateRelativeKeyFallsBack() throws IOException {
        final UnsupportedOperationException e = assertFallsBack(
            histogramMapping(),
            "{\"h\":{\"values\":[1.0],\"counts\":[1]},\"h.values\":[2.0]}"
        );
        assertThat(e.getMessage(), containsString("duplicate [values] key"));
    }

    /**
     * A document carrying only one of the two keys while another carries both. Both leaves exist here, so this
     * covers the per-row presence check rather than the missing-key one in {@link #testGroupBailOuts}.
     */
    public void testPartialHistogramInMixedBatchFallsBack() throws IOException {
        final UnsupportedOperationException e = assertFallsBack(
            histogramMapping(),
            "{\"h\":{\"values\":[1.0],\"counts\":[1]}}",
            "{\"h\":{\"values\":[2.0]}}"
        );
        assertThat(e.getMessage(), containsString("document [1] carries only one of [values] and [counts]"));
    }

    /**
     * {@code ignore_malformed: true} does not license the batch path to accept malformed input: only the row
     * path records the value in {@code _ignored_source} and the field in {@code _ignored}.
     */
    public void testMalformedStillFallsBackUnderIgnoreMalformed() throws IOException {
        for (boolean ignoreMalformed : List.of(true, false)) {
            assertFallsBack(
                histogramMapping(b -> b.field("ignore_malformed", ignoreMalformed)),
                "{\"h\":{\"values\":[2.0,1.0],\"counts\":[1,1]}}"
            );
        }
    }

    /**
     * The exponential-histogram form is refused whatever {@code coerce} says, its keys being neither
     * {@code values} nor {@code counts}. It is the only input the row path consults {@code coerce} for —
     * converting it when {@code true}, rejecting it when {@code false} — so the batch path must stay out of both.
     */
    public void testExponentialHistogramFormFallsBack() throws IOException {
        for (boolean coerce : List.of(true, false)) {
            assertFallsBack(
                histogramMapping(b -> b.field("coerce", coerce)),
                "{\"h\":{\"scale\":0,\"sum\":3.0,\"positive\":{\"counts\":[1,2]}}}"
            );
        }
    }

    // -------------------------------------------------------------------------
    // Eligibility — index modes and mappings decide whether the batch path is taken at all.
    // -------------------------------------------------------------------------

    /** Outside strict-columnar and TSDB index modes the batch path is disabled altogether. */
    public void testNonColumnarModeFallsBack() throws IOException {
        final IndexMode mode = randomFrom(IndexMode.STANDARD, IndexMode.LOGSDB);
        final MapperService ms = createMapperService(
            Settings.builder().put(IndexSettings.MODE.getKey(), mode.getName()).build(),
            mapping(b -> b.startObject(FIELD).field("type", "histogram").endObject())
        );
        final HistogramFieldMapper mapper = (HistogramFieldMapper) ms.mappingLookup().getMapper(FIELD);
        assertFalse(
            "histogram must not take the columnar path in [" + mode + "] index mode",
            mapper.supportsColumnarParse(ms.getIndexSettings())
        );
    }

    /**
     * A {@code histogram} carrying multi-fields is refused: {@code mapColumnGroupBatch} never fans out to
     * sub-mappers, so indexing the group would silently skip them.
     */
    public void testMultiFieldsFallBack() throws IOException {
        final MapperService ms = createMapperService(
            escfEligibleSettings(),
            histogramMapping(b -> b.startObject("fields").startObject("raw").field("type", "keyword").endObject().endObject())
        );
        final HistogramFieldMapper mapper = (HistogramFieldMapper) ms.mappingLookup().getMapper(FIELD);
        assertFalse(
            "histogram must not take the columnar path when [fields] is present",
            mapper.supportsColumnarParse(ms.getIndexSettings())
        );
    }

    /** Asserts {@code source} makes the group mapper bail out, and that it bailed for the expected reason. */
    private void assertGroupBailOut(String source, String expectedMessage) throws IOException {
        final UnsupportedOperationException e = assertFallsBack(histogramMapping(), source);
        assertThat(source, e.getMessage(), containsString(expectedMessage));
    }

    /**
     * Asserts that the batch path refuses {@code sources}, one document each, so {@code ShardBatchMapper} falls back to the row
     * path. The batch runs through the full harness, so it must pass {@code ShardBatchMapper.resolveMappers} and reach the mapper.
     */
    private UnsupportedOperationException assertFallsBack(XContentBuilder mapping, String... sources) {
        final Doc[] docs = new Doc[sources.length];
        for (int i = 0; i < sources.length; i++) {
            docs[i] = histogramDoc(i + 1, sources[i]);
        }
        return expectThrows(
            UnsupportedOperationException.class,
            String.join(", ", sources),
            () -> assertColumnarMatchesXContent(mapping, escfEligibleSettings(), batch("falls back", 1L, docs))
        );
    }
}
