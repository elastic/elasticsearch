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
import org.elasticsearch.common.hash.BufferedMurmur3Hasher;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

import static org.hamcrest.Matchers.equalTo;

public class ColumnarTsidAccumulatorTests extends ESTestCase {

    /** One dimension value: a path and its already-computed 128-bit value hash. */
    private record Dim(String path, long valueH1, long valueH2) {}

    private static IndexVersion multiBytePrefixVersion() {
        return IndexVersionUtils.randomVersionBetween(
            IndexVersions.TSID_CREATED_DURING_ROUTING,
            IndexVersionUtils.getPreviousVersion(IndexVersions.TSID_SINGLE_PREFIX_BYTE_FEATURE_FLAG)
        );
    }

    private static IndexVersion singleBytePrefixVersion() {
        return IndexVersionUtils.randomVersionOnOrAfter(IndexVersions.TSID_SINGLE_PREFIX_BYTE);
    }

    private static void assertAccumulatorMatchesTsidBuilder(List<List<Dim>> rows) {
        for (IndexVersion version : List.of(multiBytePrefixVersion(), singleBytePrefixVersion())) {
            boolean singleByte = TsidBuilder.useSingleBytePrefixLayout(version);
            BufferedMurmur3Hasher hasher = new BufferedMurmur3Hasher(0L);

            // Reference: row-major through TsidBuilder, dimensions added in source order.
            BytesRef[] expected = new BytesRef[rows.size()];
            for (int r = 0; r < rows.size(); r++) {
                TsidBuilder builder = TsidBuilder.newBuilder();
                for (Dim d : rows.get(r)) {
                    MurmurHash3.Hash128 pathHash = TsidBuilder.hashPath(hasher, d.path());
                    builder.addPrehashedDimension(d.path(), pathHash.h1, pathHash.h2, d.valueH1(), d.valueH2());
                }
                expected[r] = builder.buildTsid(version);
            }

            // Under test: column-major folding. Distinct paths sorted, one path group each.
            var sortedPaths = new TreeSet<String>();
            for (List<Dim> row : rows) {
                for (Dim d : row) {
                    sortedPaths.add(d.path());
                }
            }
            ColumnarTsidAccumulator accumulator = new ColumnarTsidAccumulator(rows.size(), singleByte);
            int pathGroup = TsidBuilder.NO_PATH_GROUP;
            for (String path : sortedPaths) {
                pathGroup++;
                MurmurHash3.Hash128 pathHash = TsidBuilder.hashPath(hasher, path);
                int prefixRank = TsidBuilder.prefixByteRank(path);
                for (int r = 0; r < rows.size(); r++) {
                    for (Dim d : rows.get(r)) {
                        if (d.path().equals(path)) {
                            accumulator.add(r, pathHash.h1, pathHash.h2, d.valueH1(), d.valueH2(), pathGroup, prefixRank);
                        }
                    }
                }
            }
            BytesRef[] actual = accumulator.build();

            assertThat("row count, version " + version, actual.length, equalTo(rows.size()));
            for (int r = 0; r < rows.size(); r++) {
                assertThat("row " + r + ", version " + version, actual[r], equalTo(expected[r]));
            }
        }
    }

    private static Dim randomDim(String path) {
        return new Dim(path, randomLong(), randomLong());
    }

    /**
     * The name-similarity stream contributes 8 bytes per dimension, so it is block aligned only for
     * even dimension counts and carries a pending half-block otherwise. An inverted parity branch is
     * correct for exactly half of these counts, so every count in the range is checked.
     */
    public void testDimensionCountParity() {
        for (int n = 1; n <= 9; n++) {
            List<Dim> row = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                row.add(randomDim("dim.f" + i));
            }
            assertAccumulatorMatchesTsidBuilder(List.of(row));
        }
    }

    /** Rows in one batch with different dimension counts exercise per-row tail parity independently. */
    public void testMixedDimensionCountsInOneBatch() {
        List<List<Dim>> rows = new ArrayList<>();
        for (int n = 1; n <= 6; n++) {
            List<Dim> row = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                row.add(randomDim("dim.f" + i));
            }
            rows.add(row);
        }
        assertAccumulatorMatchesTsidBuilder(rows);
    }

    /**
     * Repeated values on one path (an array dimension) must contribute one value-similarity byte, not
     * one per element, while still folding every value into the full hash.
     */
    public void testArrayValuesShareOnePathGroup() {
        for (int arrayLength : new int[] { 2, 3, 5, 7 }) {
            List<Dim> row = new ArrayList<>();
            for (int i = 0; i < arrayLength; i++) {
                row.add(randomDim("dim.tags"));
            }
            row.add(randomDim("dim.host"));
            assertAccumulatorMatchesTsidBuilder(List.of(row));
        }
    }

    /**
     * More distinct paths than {@link TsidBuilder#MAX_TSID_VALUE_SIMILARITY_FIELDS}, so the
     * value-similarity cap binds and the multi-byte layout's emitted count is below its capacity.
     */
    public void testMoreDistinctPathsThanSimilarityCap() {
        List<Dim> row = new ArrayList<>();
        for (int i = 0; i < TsidBuilder.MAX_TSID_VALUE_SIMILARITY_FIELDS + 3; i++) {
            row.add(randomDim("dim.f" + i));
        }
        assertAccumulatorMatchesTsidBuilder(List.of(row));
    }

    /**
     * An array long enough to fill the similarity cap on a single path: the multi-byte layout emits
     * one byte while the backing array is sized for four, so returned length and capacity diverge.
     */
    public void testSingleArrayPathLongerThanSimilarityCap() {
        List<Dim> row = new ArrayList<>();
        for (int i = 0; i < TsidBuilder.MAX_TSID_VALUE_SIMILARITY_FIELDS + 2; i++) {
            row.add(randomDim("dim.tags"));
        }
        assertAccumulatorMatchesTsidBuilder(List.of(row));
    }

    public void testOtelMetricNamesHashDrivesPrefixByte() {
        assertAccumulatorMatchesTsidBuilder(List.of(List.of(randomDim(TsidBuilder.OTEL_METRIC_FIELD), randomDim("dim.host"))));
    }

    public void testPrometheusLabelDrivesPrefixByte() {
        assertAccumulatorMatchesTsidBuilder(List.of(List.of(randomDim(TsidBuilder.PROMETHEUS_LABEL_FIELD), randomDim("dim.host"))));
    }

    /** OTel outranks Prometheus when both are present, regardless of sorted order. */
    public void testOtelTakesPrecedenceOverPrometheus() {
        List<Dim> row = List.of(
            randomDim(TsidBuilder.OTEL_METRIC_FIELD),
            randomDim(TsidBuilder.PROMETHEUS_LABEL_FIELD),
            randomDim("dim.host")
        );
        assertAccumulatorMatchesTsidBuilder(List.of(row));
    }

    /**
     * An array-valued special dimension must contribute its <em>first</em> value to the prefix byte.
     * A non-strict rank comparison would pick the last and is not caught by single-valued cases.
     *
     * <p>Repeated over several value sets on purpose: this divergence shows up in the single prefix
     * byte alone, so any one case has a ~1/256 chance of colliding on the wrong value.
     */
    public void testArrayValuedSpecialDimensionUsesFirstValue() {
        for (String specialPath : List.of(TsidBuilder.OTEL_METRIC_FIELD, TsidBuilder.PROMETHEUS_LABEL_FIELD)) {
            for (int trial = 0; trial < 8; trial++) {
                List<Dim> row = List.of(randomDim(specialPath), randomDim(specialPath), randomDim(specialPath), randomDim("dim.host"));
                assertAccumulatorMatchesTsidBuilder(List.of(row));
            }
        }
    }

    /**
     * A batch where only some rows carry the special dimension: rows with it take the value-hash
     * prefix, rows without it fall back to the name-similarity stream. Both must be right at once.
     */
    public void testSpecialDimensionPresentOnlyOnSomeRows() {
        List<List<Dim>> rows = List.of(
            List.of(randomDim(TsidBuilder.OTEL_METRIC_FIELD), randomDim("dim.host")),
            List.of(randomDim("dim.host"), randomDim("dim.region")),
            List.of(randomDim(TsidBuilder.PROMETHEUS_LABEL_FIELD), randomDim("dim.host")),
            List.of(randomDim("dim.host"))
        );
        assertAccumulatorMatchesTsidBuilder(rows);
    }

    /** Randomized batches over the whole shape space. */
    public void testRandomized() {
        int rowCount = randomIntBetween(1, 12);
        List<String> paths = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 7); i++) {
            paths.add("dim.f" + i);
        }
        if (randomBoolean()) {
            paths.add(TsidBuilder.OTEL_METRIC_FIELD);
        }
        if (randomBoolean()) {
            paths.add(TsidBuilder.PROMETHEUS_LABEL_FIELD);
        }

        List<List<Dim>> rows = new ArrayList<>(rowCount);
        for (int r = 0; r < rowCount; r++) {
            List<Dim> row = new ArrayList<>();
            for (String path : paths) {
                // Each path is absent, single valued, or multi valued for this row.
                int values = randomIntBetween(0, 3);
                for (int v = 0; v < values; v++) {
                    row.add(randomDim(path));
                }
            }
            if (row.isEmpty()) {
                row.add(randomDim(paths.get(0)));  // an empty row would throw, which is tested elsewhere
            }
            // Source order within a row need not be sorted; TsidBuilder sorts, and the columnar path
            // reaches the same order by visiting columns sorted by path.
            row.sort(Comparator.comparing(Dim::path));
            rows.add(row);
        }
        assertAccumulatorMatchesTsidBuilder(rows);
    }

    public void testRowWithNoDimensionsThrows() {
        ColumnarTsidAccumulator accumulator = new ColumnarTsidAccumulator(2, randomBoolean());
        MurmurHash3.Hash128 pathHash = TsidBuilder.hashPath(new BufferedMurmur3Hasher(0L), "dim.host");
        // Row 0 gets a value, row 1 gets nothing.
        accumulator.add(0, pathHash.h1, pathHash.h2, randomLong(), randomLong(), 0, TsidBuilder.PREFIX_RANK_NONE);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, accumulator::build);
        assertThat(e.getMessage(), equalTo("Dimensions are empty"));
    }
}
