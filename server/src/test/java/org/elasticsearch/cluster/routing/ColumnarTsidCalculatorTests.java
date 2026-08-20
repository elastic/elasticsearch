/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_VERSION_CREATED;
import static org.hamcrest.Matchers.equalTo;

/**
 * Parity tests for {@link ColumnarTsidCalculator} against the source-parser reference path
 * ({@link IndexRouting.ExtractFromSource.ForIndexDimensions#buildTsid(XContentType, BytesReference)}).
 *
 * <p>The columnar tsid for each row must be byte-identical to the single-doc source-parser tsid,
 * since both feed the same {@link TsidBuilder}. Tests cover all dimension types, arrays, explicit
 * nulls, UNION columns, sparse batches, the skip bitset, and both tsid layouts.
 */
public class ColumnarTsidCalculatorTests extends ESTestCase {

    // ── helpers ─────────────────────────────────────────────────────────────

    private static IndexRouting.ExtractFromSource.ForIndexDimensions forIndexDimensions(String dimensionPath) {
        Settings settings = Settings.builder()
            .put(SETTING_INDEX_VERSION_CREATED.getKey(), IndexVersion.current())
            .put(IndexMetadata.INDEX_DIMENSIONS.getKey(), dimensionPath)
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .build();
        IndexMetadata md = IndexMetadata.builder("test").settings(settings).numberOfShards(8).numberOfReplicas(0).build();
        IndexRouting routing = IndexRouting.fromIndexMetadata(md);
        return (IndexRouting.ExtractFromSource.ForIndexDimensions) routing;
    }

    private static BytesReference toJson(Map<String, Object> doc) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.map(doc);
            return BytesReference.bytes(builder);
        }
    }

    /**
     * Builds a batch from {@code sources}, computes tsids via the columnar calculator, and asserts
     * byte-for-byte equality with the per-doc source-parser tsid for every row.
     */
    private static void assertColumnarMatchesSourceParser(
        List<BytesReference> sources,
        IndexRouting.ExtractFromSource.ForIndexDimensions strategy
    ) throws IOException {
        // Reference: per-doc source-parser path
        BytesRef[] expected = new BytesRef[sources.size()];
        for (int i = 0; i < sources.size(); i++) {
            expected[i] = strategy.buildTsid(XContentType.JSON, sources.get(i));
        }

        // Columnar path
        EscfBatch batch = EscfEncoder.encode(sources, XContentType.JSON);
        BytesRef[] actual = ColumnarTsidCalculator.computeTsids(batch, strategy::matchesField, strategy.creationVersion);

        assertThat("result length", actual.length, equalTo(sources.size()));
        for (int i = 0; i < sources.size(); i++) {
            assertThat("tsid mismatch at row " + i, actual[i], equalTo(expected[i]));
        }
    }

    // ── test cases ──────────────────────────────────────────────────────────

    public void testFlatStringDimensions() throws IOException {
        var strategy = forIndexDimensions("dim.*");
        List<BytesReference> sources = List.of(
            toJson(Map.of("dim.host", "node-7", "dim.region", "us-west-2", "metric", "cpu")),
            toJson(Map.of("dim.host", "node-3", "dim.region", "eu-central-1", "metric", "mem"))
        );
        assertColumnarMatchesSourceParser(sources, strategy);
    }

    public void testNestedObjectDimensions() throws IOException {
        var strategy = forIndexDimensions("dim.*");
        // Nested object: EscfEncoder expands "dim.host" and "dim.region" from the nested structure.
        List<BytesReference> sources = List.of(
            toJson(Map.of("dim", Map.of("host", "alpha", "region", "us"), "value", 1)),
            toJson(Map.of("dim", Map.of("host", "beta", "region", "eu"), "value", 2))
        );
        assertColumnarMatchesSourceParser(sources, strategy);
    }

    public void testWildcardDimensionPaths() throws IOException {
        // "attributes.*" matches any field under attributes.
        var strategy = forIndexDimensions("attributes.*");
        List<BytesReference> sources = List.of(
            toJson(Map.of("attributes", Map.of("env", "prod", "service", "web"), "ts", 1)),
            toJson(Map.of("attributes", Map.of("env", "staging", "service", "api"), "ts", 2))
        );
        assertColumnarMatchesSourceParser(sources, strategy);
    }

    public void testIntegerAndLongDimensions() throws IOException {
        var strategy = forIndexDimensions("dim.*");
        List<BytesReference> sources = List.of(
            toJson(Map.of("dim.shard", 7, "dim.epoch", 1700000000L)),
            toJson(Map.of("dim.shard", 3, "dim.epoch", 1800000000L))
        );
        assertColumnarMatchesSourceParser(sources, strategy);
    }

    public void testFloatingPointDimensions() throws IOException {
        // 1.5 can be represented as float exactly — EscfEncoder narrows it to FLOAT; the calculator
        // must still produce the same tsid as the funnel which sees DOUBLE.
        // 0.1 cannot be exactly represented as float — EscfEncoder keeps it as DOUBLE.
        var strategy = forIndexDimensions("dim.*");
        List<BytesReference> sources = List.of(toJson(Map.of("dim.weight", 1.5, "dim.fraction", 0.1, "dim.ratio", 3.14)));
        assertColumnarMatchesSourceParser(sources, strategy);
    }

    public void testBooleanDimensions() throws IOException {
        var strategy = forIndexDimensions("dim.*");
        List<BytesReference> sources = List.of(
            toJson(Map.of("dim.active", true, "dim.archived", false)),
            toJson(Map.of("dim.active", false, "dim.archived", true))
        );
        assertColumnarMatchesSourceParser(sources, strategy);
    }

    public void testExplicitNullsAreSkipped() throws IOException {
        // A null at a dimension field contributes no entry; the tsid is computed from the other dims.
        var strategy = forIndexDimensions("dim.*");
        // Build JSON with an explicit null manually so the null is preserved in the source bytes.
        BytesReference src0;
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            b.startObject();
            b.field("dim.host", "h1");
            b.nullField("dim.note");
            b.endObject();
            src0 = BytesReference.bytes(b);
        }
        BytesReference src1;
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            b.startObject();
            b.field("dim.host", "h2");
            b.nullField("dim.other");
            b.endObject();
            src1 = BytesReference.bytes(b);
        }
        assertColumnarMatchesSourceParser(List.of(src0, src1), strategy);
    }

    public void testArrayDimension() throws IOException {
        // Arrays at dimension fields: each element is a separate builder entry; order matters.
        var strategy = forIndexDimensions("dim.*");
        BytesReference src0;
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            b.startObject().field("dim.host", "h1").array("dim.tags", "a", "b", "c").endObject();
            src0 = BytesReference.bytes(b);
        }
        BytesReference src1;
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            b.startObject().field("dim.host", "h2").array("dim.tags", "x", "y").endObject();
            src1 = BytesReference.bytes(b);
        }
        assertColumnarMatchesSourceParser(List.of(src0, src1), strategy);
    }

    public void testArrayOrderSensitivity() throws IOException {
        // [a, b] must produce a different tsid from [b, a] at the same path.
        var strategy = forIndexDimensions("dim.*");
        BytesReference ab;
        BytesReference ba;
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            b.startObject().array("dim.tags", "a", "b").endObject();
            ab = BytesReference.bytes(b);
        }
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            b.startObject().array("dim.tags", "b", "a").endObject();
            ba = BytesReference.bytes(b);
        }
        EscfBatch batch = EscfEncoder.encode(List.of(ab, ba), XContentType.JSON);
        BytesRef[] tsids = ColumnarTsidCalculator.computeTsids(batch, strategy::matchesField, strategy.creationVersion);
        assertThat("array order must affect tsid", tsids[0].equals(tsids[1]), equalTo(false));
        // And each individually matches the reference.
        assertThat(tsids[0], equalTo(strategy.buildTsid(XContentType.JSON, ab)));
        assertThat(tsids[1], equalTo(strategy.buildTsid(XContentType.JSON, ba)));
    }

    public void testSparseBatch() throws IOException {
        // Some rows lack a dimension that is present in others — exercises the validity bitset path.
        var strategy = forIndexDimensions("dim.*");
        List<BytesReference> sources = List.of(
            toJson(Map.of("dim.host", "a", "dim.region", "us")),
            toJson(Map.of("dim.host", "b")),                           // dim.region absent
            toJson(Map.of("dim.host", "c", "dim.region", "eu"))
        );
        assertColumnarMatchesSourceParser(sources, strategy);
    }

    public void testUnionColumn() throws IOException {
        // Same field path with different types across docs → EscfEncoder produces a UNION column.
        var strategy = forIndexDimensions("dim.*");
        List<BytesReference> sources = List.of(
            toJson(Map.of("dim.x", "hello", "dim.y", "anchor")),   // dim.x = STRING
            toJson(Map.of("dim.x", 42, "dim.y", "anchor")),        // dim.x = INT
            toJson(Map.of("dim.x", true, "dim.y", "anchor"))       // dim.x = BOOL
        );
        assertColumnarMatchesSourceParser(sources, strategy);
    }

    public void testUnionColumnWithExplicitNull() throws IOException {
        // A NULL entry in a UNION column is skipped (same as explicit null in source).
        var strategy = forIndexDimensions("dim.*");
        BytesReference withNull;
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            b.startObject().field("dim.anchor", "a").nullField("dim.x").endObject();
            withNull = BytesReference.bytes(b);
        }
        BytesReference withValue;
        try (XContentBuilder b = XContentFactory.jsonBuilder()) {
            b.startObject().field("dim.anchor", "a").field("dim.x", "present").endObject();
            withValue = BytesReference.bytes(b);
        }
        // Mix null and value rows so dim.x becomes UNION with a NULL entry in one row.
        assertColumnarMatchesSourceParser(List.of(withNull, withValue), strategy);
    }

    public void testSingleRowBatch() throws IOException {
        var strategy = forIndexDimensions("dim.*");
        List<BytesReference> sources = List.of(toJson(Map.of("dim.host", "solo", "dim.region", "ap")));
        assertColumnarMatchesSourceParser(sources, strategy);
    }

    public void testManyIdenticalRows() throws IOException {
        // When all rows have the same dimension values all tsids should be equal.
        var strategy = forIndexDimensions("dim.*");
        BytesReference src = toJson(Map.of("dim.host", "same", "dim.region", "us"));
        int n = randomIntBetween(5, 20);
        List<BytesReference> sources = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            sources.add(src);
        }
        EscfBatch batch = EscfEncoder.encode(sources, XContentType.JSON);
        BytesRef[] tsids = ColumnarTsidCalculator.computeTsids(batch, strategy::matchesField, strategy.creationVersion);
        BytesRef first = tsids[0];
        for (int i = 1; i < n; i++) {
            assertThat("all identical rows must have equal tsid", tsids[i], equalTo(first));
        }
    }

    public void testRowWithNoDimensionValuesThrows() throws IOException {
        // A doc with no matching dimension fields causes buildTsid to throw IAE.
        var strategy = forIndexDimensions("dim.*");
        List<BytesReference> sources = List.of(toJson(Map.of("metric", 42)));   // no "dim.*" field
        EscfBatch batch = EscfEncoder.encode(sources, XContentType.JSON);
        expectThrows(
            IllegalArgumentException.class,
            () -> ColumnarTsidCalculator.computeTsids(batch, strategy::matchesField, strategy.creationVersion)
        );
    }

    public void testBothTsidLayouts() throws IOException {
        // The columnar tsid must match the reference for both the multi-byte-prefix (pre-feature-flag)
        // and single-byte-prefix (post-feature-flag) layouts.
        for (IndexVersion version : List.of(
            IndexVersionUtils.randomPreviousCompatibleVersion(IndexVersions.TSID_SINGLE_PREFIX_BYTE_FEATURE_FLAG),
            IndexVersionUtils.randomVersionOnOrAfter(IndexVersions.TSID_SINGLE_PREFIX_BYTE_FEATURE_FLAG)
        )) {
            Settings settings = Settings.builder()
                .put(SETTING_INDEX_VERSION_CREATED.getKey(), version)
                .put(IndexMetadata.INDEX_DIMENSIONS.getKey(), "dim.*")
                .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                .build();
            // TSID_CREATED_DURING_ROUTING is required by ForIndexDimensions; skip older versions that don't satisfy it.
            if (version.before(IndexVersions.TSID_CREATED_DURING_ROUTING)) {
                continue;
            }
            IndexMetadata md = IndexMetadata.builder("test").settings(settings).numberOfShards(8).numberOfReplicas(0).build();
            IndexRouting.ExtractFromSource.ForIndexDimensions strategy = (IndexRouting.ExtractFromSource.ForIndexDimensions) IndexRouting
                .fromIndexMetadata(md);
            List<BytesReference> sources = List.of(
                toJson(Map.of("dim.host", "n1", "dim.region", "us")),
                toJson(Map.of("dim.host", "n2", "dim.region", "eu"))
            );
            assertColumnarMatchesSourceParser(sources, strategy);
        }
    }

    public void testComputesAllRows() throws IOException {
        // computeTsids processes every row — there is no skip mechanism.
        var strategy = forIndexDimensions("dim.*");
        List<BytesReference> sources = List.of(
            toJson(Map.of("dim.host", "a", "dim.region", "us")),
            toJson(Map.of("dim.host", "b", "dim.region", "eu")),
            toJson(Map.of("dim.host", "c", "dim.region", "ap"))
        );
        assertColumnarMatchesSourceParser(sources, strategy);
    }

    public void testNonEscfBatchThrows() {
        var strategy = forIndexDimensions("dim.*");
        // Minimal anonymous SourceBatch that is not an EscfBatch.
        SourceBatch notEscf = new SourceBatch() {
            @Override
            public int docCount() {
                return 0;
            }

            @Override
            public SourceSchema schema() {
                return null;
            }

            @Override
            public int columnCount() {
                return 0;
            }

            @Override
            public org.elasticsearch.common.bytes.BytesReference data() {
                return null;
            }

            @Override
            public org.elasticsearch.sourcebatch.SourceRow row(int docIndex) {
                return null;
            }

            @Override
            public SourceBatch slice(int from, int to) {
                return this;
            }

            @Override
            public void close() {}

            @Override
            public long ramBytesUsed() {
                return 0;
            }
        };
        expectThrows(
            UnsupportedOperationException.class,
            () -> ColumnarTsidCalculator.computeTsids(notEscf, strategy::matchesField, strategy.creationVersion)
        );
    }

    public void testRandomized() throws IOException {
        // Random docs of random dimension shapes, batch vs per-doc parity.
        var strategy = forIndexDimensions("dim.*");
        int docCount = randomIntBetween(1, 20);
        List<BytesReference> sources = new ArrayList<>(docCount);
        for (int i = 0; i < docCount; i++) {
            try (XContentBuilder b = XContentFactory.jsonBuilder()) {
                b.startObject();
                int dims = randomIntBetween(1, 5);
                for (int d = 0; d < dims; d++) {
                    String key = "dim." + randomAlphaOfLengthBetween(1, 6);
                    switch (randomInt(3)) {
                        case 0 -> b.field(key, randomAlphaOfLengthBetween(1, 16));
                        case 1 -> b.field(key, randomInt());
                        case 2 -> b.field(key, randomBoolean());
                        case 3 -> b.field(key, randomDoubleBetween(-1e6, 1e6, true));
                    }
                }
                b.field("ts", i);
                b.endObject();
                sources.add(BytesReference.bytes(b));
            }
        }
        assertColumnarMatchesSourceParser(sources, strategy);
    }
}
