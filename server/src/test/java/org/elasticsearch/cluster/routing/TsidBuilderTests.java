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
import org.elasticsearch.xcontent.Text;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class TsidBuilderTests extends ESTestCase {

    private static IndexVersion randomMultiplePrefixBytesVersion() {
        return IndexVersionUtils.randomPreviousCompatibleVersion(IndexVersions.TSID_SINGLE_PREFIX_BYTE_FEATURE_FLAG);
    }

    private static IndexVersion randomSinglePrefixByteVersion() {
        return IndexVersionUtils.randomVersionOnOrAfter(IndexVersions.TSID_SINGLE_PREFIX_BYTE_FEATURE_FLAG);
    }

    /**
     * A builder reused via {@link TsidBuilder#reset()} — as {@code DimensionsExtractor} does on the
     * batch path — must produce exactly what a fresh builder would, since state carried across builds
     * (the hasher, and the multi-byte layout's value-similarity staging buffer) outlives a single
     * build. The second shape is array-valued, so its dimension count exceeds the number of
     * value-similarity bytes it emits and the backing array is deliberately larger than the returned
     * slice.
     */
    public void testReusedBuilderMatchesFreshBuilder() {
        for (IndexVersion version : List.of(randomMultiplePrefixBytesVersion(), randomSinglePrefixByteVersion())) {
            TsidBuilder reused = TsidBuilder.newBuilder();

            // Fills every value-similarity slot: 5 distinct paths against a cap of 4.
            BytesRef first = addDistinctPaths(reused).buildTsid(version);
            assertThat(first, equalTo(addDistinctPaths(TsidBuilder.newBuilder()).buildTsid(version)));

            // 5 dimensions collapsing onto 1 path, so only one value-similarity byte is emitted.
            reused.reset();
            BytesRef second = addRepeatedPath(reused).buildTsid(version);
            assertThat(second, equalTo(addRepeatedPath(TsidBuilder.newBuilder()).buildTsid(version)));
            assertThat(second, not(equalTo(first)));

            // And back again, so the shrink/grow transition is covered in both directions.
            reused.reset();
            assertThat(addDistinctPaths(reused).buildTsid(version), equalTo(first));
        }
    }

    private static TsidBuilder addDistinctPaths(TsidBuilder builder) {
        return builder.addStringDimension("a", "1")
            .addStringDimension("b", "2")
            .addStringDimension("c", "3")
            .addStringDimension("d", "4")
            .addStringDimension("e", "5");
    }

    private static TsidBuilder addRepeatedPath(TsidBuilder builder) {
        return builder.addStringDimension("a", "1")
            .addStringDimension("a", "2")
            .addStringDimension("a", "3")
            .addStringDimension("a", "4")
            .addStringDimension("a", "5");
    }

    public void testAddDimensions() {
        TsidBuilder builder = TsidBuilder.newBuilder()
            .addStringDimension("test_string", "hello")
            .addBooleanDimension("test_bool", true)
            .addIntDimension("test_int", 42)
            .addLongDimension("test_long", 123456789L)
            .addDoubleDimension("test_double", 3.14159)
            .addStringDimension("test_array", "value1")
            .addStringDimension("test_array", "value2");

        // if these change, we'll need a new index version
        // because it means existing time series will get a new _tsid and will be routed to a different shard
        assertThat(builder.hash().toString(), equalTo("0xd4de1356065d297a2be489781e15d256"));
        BytesRef legacyTsid = builder.buildTsid(randomMultiplePrefixBytesVersion());
        assertThat(legacyTsid.length, equalTo(21));
        assertThat(
            HexFormat.of().formatHex(legacyTsid.bytes, legacyTsid.offset, legacyTsid.length),
            equalTo("bfa0a8d66356d2151e7889e42b7a295d065613ded4")
        );
        IndexVersion newVersion = randomSinglePrefixByteVersion();
        BytesRef newTsid = builder.buildTsid(newVersion);
        if (TsidBuilder.useSingleBytePrefixLayout(newVersion)) {
            assertThat(
                HexFormat.of().formatHex(newTsid.bytes, newTsid.offset, newTsid.length),
                equalTo("bfd2151e7889e42b7a295d065613ded4")
            );
        } else {
            assertThat(newTsid, equalTo(legacyTsid));
        }
    }

    public void testArray() {
        TsidBuilder builder = TsidBuilder.newBuilder().addStringDimension("test_non_array", "value");

        int arrayValues = randomIntBetween(32, 64);
        for (int i = 0; i < arrayValues; i++) {
            builder.addStringDimension("_test_large_array", "value_" + i);
        }
        assertThat(builder.buildTsid(randomMultiplePrefixBytesVersion()).length, equalTo(19));
        IndexVersion singleVersion = randomSinglePrefixByteVersion();
        if (TsidBuilder.useSingleBytePrefixLayout(singleVersion)) {
            assertThat(builder.buildTsid(singleVersion).length, equalTo(16));
        } else {
            assertThat(builder.buildTsid(singleVersion).length, equalTo(19));
        }
    }

    public void testOrderingOfDifferentFieldsDoesNotMatter() {
        assertEqualBuilders(
            TsidBuilder.newBuilder().addStringDimension("foo", "bar").addStringDimension("baz", "qux"),
            TsidBuilder.newBuilder().addStringDimension("baz", "qux").addStringDimension("foo", "bar")
        );
    }

    public void testOrderingOfMultiFieldsMatters() {
        IndexVersion oldVersion = randomMultiplePrefixBytesVersion();
        assertThat(
            TsidBuilder.newBuilder().addStringDimension("foo", "bar").addStringDimension("foo", "baz").buildTsid(oldVersion),
            not(equalTo(TsidBuilder.newBuilder().addStringDimension("foo", "baz").addStringDimension("foo", "bar").buildTsid(oldVersion)))
        );
        IndexVersion newVersion = randomSinglePrefixByteVersion();
        assertThat(
            TsidBuilder.newBuilder().addStringDimension("foo", "bar").addStringDimension("foo", "baz").buildTsid(newVersion),
            not(equalTo(TsidBuilder.newBuilder().addStringDimension("foo", "baz").addStringDimension("foo", "bar").buildTsid(newVersion)))
        );
    }

    public void testAddStringDimension() {
        String stringValue = randomUnicodeOfLengthBetween(0, 1024);
        BytesRef bytesRef = new BytesRef(stringValue);
        byte[] utf8Bytes = stringValue.getBytes(StandardCharsets.UTF_8);
        assertEqualBuilders(
            TsidBuilder.newBuilder().addStringDimension("test_string", stringValue),
            TsidBuilder.newBuilder().addStringDimension("test_string", new Text(stringValue).bytes()),
            TsidBuilder.newBuilder().addStringDimension("test_string", bytesRef.bytes, bytesRef.offset, bytesRef.length),
            TsidBuilder.newBuilder().addStringDimension("test_string", utf8Bytes, 0, utf8Bytes.length)
        );
    }

    private static void assertEqualBuilders(TsidBuilder... tsidBuilders) {
        IndexVersion version = randomBoolean() ? randomMultiplePrefixBytesVersion() : randomSinglePrefixByteVersion();
        assertThat(Arrays.stream(tsidBuilders).map(builder -> builder.buildTsid(version)).distinct().toList(), hasSize(1));
        assertThat(Arrays.stream(tsidBuilders).map(TsidBuilder::hash).distinct().toList(), hasSize(1));
        assertThat(tsidBuilders[0].buildTsid(version), notNullValue());
        assertThat(tsidBuilders[0].buildTsid(version).length, greaterThan(0));
    }

    public void testAddAll() {
        TsidBuilder builder1 = TsidBuilder.newBuilder().addStringDimension("foo", "bar");
        TsidBuilder builder2 = TsidBuilder.newBuilder().addStringDimension("baz", "qux");
        assertEqualBuilders(
            TsidBuilder.newBuilder().addAll(builder1).addAll(builder2),
            TsidBuilder.newBuilder().addStringDimension("foo", "bar").addStringDimension("baz", "qux")
        );
    }

    public void testAddAllWithNullOrEmpty() {
        assertEqualBuilders(
            TsidBuilder.newBuilder().addIntDimension("test", 42),
            TsidBuilder.newBuilder().addIntDimension("test", 42).addAll(null).addAll(TsidBuilder.newBuilder())
        );
    }

    public void testExceptionWhenNoDimensions() {
        TsidBuilder builder = TsidBuilder.newBuilder();
        assertThat(builder.hash(), equalTo(new MurmurHash3.Hash128()));
        for (IndexVersion version : List.of(randomMultiplePrefixBytesVersion(), randomSinglePrefixByteVersion())) {
            IllegalArgumentException tsidException = expectThrows(IllegalArgumentException.class, () -> builder.buildTsid(version));
            assertTrue(tsidException.getMessage().contains("Dimensions are empty"));
        }
    }

    public void testTsidMinSize() {
        TsidBuilder builder = TsidBuilder.newBuilder().addIntDimension("test_int", 42);
        assertThat(builder.buildTsid(randomMultiplePrefixBytesVersion()).length, equalTo(18));
    }

    public void testTsidMaxSize() {
        TsidBuilder tsidBuilder = TsidBuilder.newBuilder();
        int dimensions = randomIntBetween(4, 64);
        for (int i = 0; i < dimensions; i++) {
            tsidBuilder.addStringDimension("dimension_" + i, "value_" + i);
        }
        assertEquals(21, tsidBuilder.buildTsid(randomMultiplePrefixBytesVersion()).length);
    }

    public void testOtelSchema() {
        TsidBuilder builder = TsidBuilder.newBuilder()
            .addStringDimension("_metric_names_hash", "random1")
            .addBooleanDimension("test_bool", true)
            .addIntDimension("test_int", 42)
            .addLongDimension("test_long", 123456789L)
            .addDoubleDimension("test_double", 3.14159)
            .addStringDimension("test_array", "value1")
            .addStringDimension("test_array", "value2");
        BytesRef oldTsid = builder.buildTsid(randomMultiplePrefixBytesVersion());
        IndexVersion newVersion = randomSinglePrefixByteVersion();
        BytesRef newTsid = builder.buildTsid(newVersion);
        assertThat(
            HexFormat.of().formatHex(oldTsid.bytes, oldTsid.offset, oldTsid.length),
            equalTo("01e3a0a8d693dbccd8eed09bb80b82b55d9756c7a6")
        );
        if (TsidBuilder.useSingleBytePrefixLayout(newVersion)) {
            assertThat(
                HexFormat.of().formatHex(newTsid.bytes, newTsid.offset, newTsid.length),
                equalTo("e3dbccd8eed09bb80b82b55d9756c7a6")
            );
        } else {
            assertThat(newTsid, equalTo(oldTsid));
        }
    }

    public void testPrometheusSchema() {
        TsidBuilder builder = TsidBuilder.newBuilder()
            .addStringDimension("labels.__name__", "random1")
            .addBooleanDimension("test_bool", true)
            .addIntDimension("test_int", 42)
            .addLongDimension("test_long", 123456789L)
            .addDoubleDimension("test_double", 3.14159)
            .addStringDimension("test_array", "value1")
            .addStringDimension("test_array", "value2");
        BytesRef oldTsid = builder.buildTsid(randomMultiplePrefixBytesVersion());
        assertThat(
            HexFormat.of().formatHex(oldTsid.bytes, oldTsid.offset, oldTsid.length),
            equalTo("afe3a0a8d67821311bea0fc3c9cbd40c8047c484aa")
        );
        IndexVersion version = randomSinglePrefixByteVersion();
        BytesRef newTsid = builder.buildTsid(version);
        if (TsidBuilder.useSingleBytePrefixLayout(version)) {
            assertThat(
                HexFormat.of().formatHex(newTsid.bytes, newTsid.offset, newTsid.length),
                equalTo("e321311bea0fc3c9cbd40c8047c484aa")
            );
        } else {
            assertThat(newTsid, equalTo(oldTsid));
        }
    }

    public void testEnableSinglePrefixByte() {
        assertTrue(TsidBuilder.useSingleBytePrefixLayout(IndexVersion.current()));
    }

    public void testPrefixByteRank() {
        assertThat(TsidBuilder.prefixByteRank(TsidBuilder.OTEL_METRIC_FIELD), equalTo(0));
        assertThat(TsidBuilder.prefixByteRank(TsidBuilder.PROMETHEUS_LABEL_FIELD), equalTo(1));
        // Any ordinary path must get PREFIX_RANK_NONE, which must beat every special rank.
        assertThat(TsidBuilder.prefixByteRank(randomAlphaOfLengthBetween(1, 20)), equalTo(TsidBuilder.PREFIX_RANK_NONE));
        assertThat(TsidBuilder.PREFIX_RANK_NONE, greaterThan(1));
    }

    /**
     * When a special-field dimension wins (rank &lt; PREFIX_RANK_NONE), {@code singleBytePrefix} must
     * return the value-similarity byte — murmur3 of {@code h1 ^ h2} — not the stream-hash low byte.
     */
    public void testSingleBytePrefixSpecialFieldUsesValueSimilarity() {
        long h1 = randomLong();
        long h2 = randomLong();
        MurmurHash3.Hash128 scratch = new MurmurHash3.Hash128();
        byte expected = TsidBuilder.similarityByte(h1, h2, scratch);
        assertThat(TsidBuilder.singleBytePrefix(0, h1, h2, null, scratch), equalTo(expected));
        assertThat(TsidBuilder.singleBytePrefix(1, h1, h2, null, scratch), equalTo(expected));
    }

    /**
     * When no special field is present (rank == PREFIX_RANK_NONE), {@code singleBytePrefix} must
     * return the low byte of the pre-accumulated name-similarity hash, not a re-hash of it.
     */
    public void testSingleBytePrefixNameSimilarityPathUsesStreamHash() {
        MurmurHash3.Hash128 nameSimilarityHash = new MurmurHash3.Hash128(randomLong(), randomLong());
        MurmurHash3.Hash128 scratch = new MurmurHash3.Hash128();
        byte expected = TsidBuilder.similarityByte(nameSimilarityHash);
        assertThat(
            TsidBuilder.singleBytePrefix(TsidBuilder.PREFIX_RANK_NONE, randomLong(), randomLong(), nameSimilarityHash, scratch),
            equalTo(expected)
        );
    }

    /**
     * The prefix byte produced by {@code singleBytePrefix} with an OTel field's value hash must equal
     * the first byte of the full TSID built by the row path for the same field value, confirming that
     * the columnar prep method and {@code computeSingleBytePrefix} agree.
     */
    public void testSingleBytePrefixMatchesTsidPrefixByteForOtelField() {
        String value = randomAlphaOfLengthBetween(1, 32);
        byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
        TsidBuilder builder = TsidBuilder.newBuilder()
            .addStringDimension(TsidBuilder.OTEL_METRIC_FIELD, value)
            .addLongDimension("other", randomLong());
        BytesRef tsid = builder.buildTsid(IndexVersions.TSID_SINGLE_PREFIX_BYTE);

        MurmurHash3.Hash128 valueHash = TsidBuilder.hashStringValue(utf8, 0, utf8.length, new MurmurHash3.Hash128());
        byte prefix = TsidBuilder.singleBytePrefix(
            TsidBuilder.prefixByteRank(TsidBuilder.OTEL_METRIC_FIELD),
            valueHash.h1,
            valueHash.h2,
            null,
            new MurmurHash3.Hash128()
        );
        assertThat(prefix, equalTo(tsid.bytes[tsid.offset]));
    }

    public void testAddPrehashedDimensionMatchesTypedDimensions() {
        BufferedMurmur3Hasher hasher = new BufferedMurmur3Hasher(0L);

        String path = "dim.host";
        MurmurHash3.Hash128 pathHash = TsidBuilder.hashPath(hasher, path);

        // String dimension: value hash = murmur3(utf8 bytes)
        String strVal = randomAlphaOfLengthBetween(1, 32);
        BytesRef strRef = new BytesRef(strVal);
        hasher.reset();
        hasher.update(strRef.bytes, strRef.offset, strRef.length);
        MurmurHash3.Hash128 strValueHash = hasher.digestHash();
        assertEqualBuilders(
            TsidBuilder.newBuilder().addStringDimension("other", "anchor").addStringDimension(path, strVal),
            TsidBuilder.newBuilder()
                .addStringDimension("other", "anchor")
                .addPrehashedDimension(path, pathHash.h1, pathHash.h2, strValueHash.h1, strValueHash.h2)
        );

        // Long dimension: value hash = Hash128(1, v)
        long longVal = randomLong();
        assertEqualBuilders(
            TsidBuilder.newBuilder().addLongDimension(path, longVal),
            TsidBuilder.newBuilder().addPrehashedDimension(path, pathHash.h1, pathHash.h2, 1L, longVal)
        );

        // Boolean dimension: value hash = Hash128(3, v ? 1 : 0)
        boolean boolVal = randomBoolean();
        assertEqualBuilders(
            TsidBuilder.newBuilder().addBooleanDimension(path, boolVal),
            TsidBuilder.newBuilder().addPrehashedDimension(path, pathHash.h1, pathHash.h2, 3L, boolVal ? 1L : 0L)
        );

        // Double dimension: value hash = Hash128(2, Double.doubleToLongBits(v))
        double doubleVal = randomDoubleBetween(-1e9, 1e9, true);
        assertEqualBuilders(
            TsidBuilder.newBuilder().addDoubleDimension(path, doubleVal),
            TsidBuilder.newBuilder().addPrehashedDimension(path, pathHash.h1, pathHash.h2, 2L, Double.doubleToLongBits(doubleVal))
        );
    }
}
