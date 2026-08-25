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
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.RoutingPathFields;
import org.elasticsearch.xcontent.XContentString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A builder for creating time series identifiers (TSIDs) based on dimensions.
 * This class allows adding various types of dimensions (int, long, double, boolean, string, bytes)
 * and builds a TSID that is a hash of the dimension names and values.
 * Important properties of TSIDs are that they cluster similar time series together,
 * which helps with storage efficiency,
 * and that they minimize the risk of hash collisions.
 * At the same time, they should be short to be efficient in terms of storage and processing.
 */
public class TsidBuilder {

    private static final boolean SINGLE_PREFIX_BYTE_ENABLED = new FeatureFlag("tsid_layout_single_prefix_byte").isEnabled();

    /**
     * The maximum number of fields to use for the value similarity part of the TSID.
     * This is a trade-off between clustering similar time series together and the size of the TSID.
     * More fields improve clustering but also increase the size of the TSID.
     */
    static final int MAX_TSID_VALUE_SIMILARITY_FIELDS = 4;
    static final String OTEL_METRIC_FIELD = "_metric_names_hash";
    static final String PROMETHEUS_LABEL_FIELD = "labels.__name__";

    /** Size of the full 128-bit hash suffix shared by both tsid layouts. */
    private static final int FULL_HASH_BYTES = 16;

    static final long LONG_VALUE_TAG = 1L;
    static final long DOUBLE_VALUE_TAG = 2L;
    static final long BOOLEAN_VALUE_TAG = 3L;

    /** {@link #prefixByteRank} result for a path with no special meaning for the prefix byte. */
    static final int PREFIX_RANK_NONE = Integer.MAX_VALUE;

    /**
     * Sentinel for "no path group seen yet", used by the columnar path to identify the first value of
     * each distinct dimension path — the row path recognises that by comparing adjacent path strings.
     */
    static final int NO_PATH_GROUP = -1;

    private final BufferedMurmur3Hasher murmur3Hasher = new BufferedMurmur3Hasher(0L);

    private final List<Dimension> dimensions;

    public TsidBuilder() {
        this.dimensions = new ArrayList<>();
    }

    public TsidBuilder(int size) {
        this.dimensions = new ArrayList<>(size);
    }

    /**
     * Clears all accumulated dimensions so this builder can be reused for another tsid.
     * The underlying dimensions list retains its capacity.
     */
    public void reset() {
        murmur3Hasher.reset();
        dimensions.clear();
    }

    public static TsidBuilder newBuilder() {
        return new TsidBuilder();
    }

    /**
     * Adds an integer dimension to the TSID.
     *
     * @param path  the path of the dimension
     * @param value the integer value of the dimension
     * @return the TsidBuilder instance for method chaining
     */
    public TsidBuilder addIntDimension(String path, int value) {
        addDimension(path, new MurmurHash3.Hash128(LONG_VALUE_TAG, value));
        return this;
    }

    /**
     * Adds a long dimension to the TSID.
     *
     * @param path  the path of the dimension
     * @param value the long value of the dimension
     * @return the TsidBuilder instance for method chaining
     */
    public TsidBuilder addLongDimension(String path, long value) {
        addDimension(path, new MurmurHash3.Hash128(LONG_VALUE_TAG, value));
        return this;
    }

    /**
     * Adds a double dimension to the TSID.
     *
     * @param path  the path of the dimension
     * @param value the double value of the dimension
     * @return the TsidBuilder instance for method chaining
     */
    public TsidBuilder addDoubleDimension(String path, double value) {
        addDimension(path, new MurmurHash3.Hash128(DOUBLE_VALUE_TAG, Double.doubleToLongBits(value)));
        return this;
    }

    /**
     * Adds a boolean dimension to the TSID.
     *
     * @param path  the path of the dimension
     * @param value the boolean value of the dimension
     * @return the TsidBuilder instance for method chaining
     */
    public TsidBuilder addBooleanDimension(String path, boolean value) {
        addDimension(path, new MurmurHash3.Hash128(BOOLEAN_VALUE_TAG, value ? 1 : 0));
        return this;
    }

    /**
     * Adds a string dimension to the TSID.
     *
     * @param path  the path of the dimension
     * @param value the string value of the dimension
     * @return the TsidBuilder instance for method chaining
     */
    public TsidBuilder addStringDimension(String path, String value) {
        addStringDimension(path, new BytesRef(value));
        return this;
    }

    private void addStringDimension(String path, BytesRef value) {
        addStringDimension(path, value.bytes, value.offset, value.length);
    }

    /**
     * Adds a string dimension to the TSID.
     *
     * @param path  the path of the dimension
     * @param value the UTF8Bytes value of the dimension
     * @return the TsidBuilder instance for method chaining
     */
    public TsidBuilder addStringDimension(String path, XContentString.UTF8Bytes value) {
        addStringDimension(path, value.bytes(), value.offset(), value.length());
        return this;
    }

    /**
     * Adds a string dimension to the TSID using a byte array.
     * The value is provided as UTF-8 encoded bytes[].
     *
     * @param path the path of the dimension
     * @param utf8Bytes the UTF-8 encoded bytes of the string value
     * @param offset the offset in the byte array where the string starts
     * @param length the length of the string in bytes
     * @return the TsidBuilder instance for method chaining
     */
    public TsidBuilder addStringDimension(String path, byte[] utf8Bytes, int offset, int length) {
        // A fresh Hash128: the result is retained by the Dimension added below, so it must not be a
        // reused scratch instance.
        addDimension(path, hashStringValue(utf8Bytes, offset, length, new MurmurHash3.Hash128()));
        return this;
    }

    /**
     * Adds a value to the TSID using a funnel.
     * This allows for complex types to be added to the TSID.
     *
     * @param value  the value to add
     * @param funnel the funnel that describes how to add the value
     * @param <T>    the type of the value
     * @return the TsidBuilder instance for method chaining
     */
    public <T> TsidBuilder add(T value, TsidFunnel<T> funnel) {
        funnel.add(value, this);
        return this;
    }

    /**
     * Adds a value to the TSID using a funnel that can throw exceptions.
     * This allows for complex types to be added to the TSID.
     *
     * @param value  the value to add
     * @param funnel the funnel that describes how to add the value
     * @param <T>    the type of the value
     * @param <E>    the type of exception that can be thrown
     * @return the TsidBuilder instance for method chaining
     * @throws E if an exception occurs while adding the value
     */
    public <T, E extends Exception> TsidBuilder add(T value, ThrowingTsidFunnel<T, E> funnel) throws E {
        funnel.add(value, this);
        return this;
    }

    /**
     * Adds a dimension whose path hash has already been computed, bypassing path re-hashing.
     *
     * <p>The {@code path} string must still be supplied because {@link #buildTsid(IndexVersion)}
     * uses it for the OTel / Prometheus prefix-byte special-case and the array-dedup guard in the
     * multi-byte layout. The path hash values must have been computed via
     * {@link #hashPath(BufferedMurmur3Hasher, String)} with the same {@code path}, so the columnar
     * path can never silently diverge from the per-row path.
     *
     * @param path   full dotted dimension path (required even though it is not re-hashed)
     * @param pathH1 first 64-bit word of the path murmur3-128 hash
     * @param pathH2 second 64-bit word of the path murmur3-128 hash
     * @param valueH1 first 64-bit word of the value murmur3-128 hash
     * @param valueH2 second 64-bit word of the value murmur3-128 hash
     * @return this builder for chaining
     */
    public TsidBuilder addPrehashedDimension(String path, long pathH1, long pathH2, long valueH1, long valueH2) {
        dimensions.add(
            new Dimension(path, new MurmurHash3.Hash128(pathH1, pathH2), new MurmurHash3.Hash128(valueH1, valueH2), dimensions.size())
        );
        return this;
    }

    /**
     * Computes the murmur3-128 hash of a dimension path string.
     *
     * <p>Extracted as a static so that {@link ColumnarTsidCalculator} can precompute the path
     * hash once per column (rather than once per value per row) and pass it to
     * {@link #addPrehashedDimension}. Using the same hasher instance as {@link #addDimension}
     * guarantees that the two call-sites produce identical hashes.
     *
     * @param hasher a shared {@link BufferedMurmur3Hasher} (will be reset before use)
     * @param path   the dimension path string
     * @return the 128-bit hash of the path
     */
    static MurmurHash3.Hash128 hashPath(BufferedMurmur3Hasher hasher, String path) {
        hasher.reset();
        hasher.addString(path);
        return hasher.digestHash();
    }

    private void addDimension(String path, MurmurHash3.Hash128 valueHash) {
        MurmurHash3.Hash128 pathHash = hashPath(murmur3Hasher, path);
        dimensions.add(new Dimension(path, pathHash, valueHash, dimensions.size()));
    }

    /**
     * Adds all dimensions from another TsidBuilder to this one.
     * If the other builder is null or has no dimensions, this method does nothing.
     *
     * @param other the other TsidBuilder to add dimensions from
     * @return this TsidBuilder instance for method chaining
     */
    public TsidBuilder addAll(TsidBuilder other) {
        if (other == null || other.dimensions.isEmpty()) {
            return this;
        }
        dimensions.addAll(other.dimensions);
        return this;
    }

    /**
     * Computes the hash of the dimensions added to this builder.
     * The hash is a 128-bit value that is computed based on the dimension names and values.
     *
     * @return a HashValue128 representing the hash of the dimensions
     * @throws IllegalArgumentException if no dimensions have been added
     */
    public MurmurHash3.Hash128 hash() {
        Collections.sort(dimensions);
        murmur3Hasher.reset();
        for (Dimension dim : dimensions) {
            murmur3Hasher.addLongs(dim.pathHash.h1, dim.pathHash.h2, dim.valueHash.h1, dim.valueHash.h2);
        }
        return murmur3Hasher.digestHash();
    }

    public final BytesRef buildTsid(IndexVersion indexVersion) {
        if (useSingleBytePrefixLayout(indexVersion)) {
            return buildSingleBytePrefixTsid();
        } else {
            return buildMultiBytePrefixTsid();
        }
    }

    public static boolean useSingleBytePrefixLayout(IndexVersion indexVersion) {
        return indexVersion.onOrAfter(IndexVersions.TSID_SINGLE_PREFIX_BYTE)
            || (SINGLE_PREFIX_BYTE_ENABLED && indexVersion.onOrAfter(IndexVersions.TSID_SINGLE_PREFIX_BYTE_FEATURE_FLAG));
    }

    /**
     * Builds a time series identifier (TSID) based on the dimensions added to this builder.
     * This is a slight adaptation of {@link RoutingPathFields#buildHash()} but creates shorter tsids.
     * The TSID is a hash that includes:
     * <ul>
     *     <li>
     *         A hash of the dimension field names (1 byte).
     *         This is to cluster time series that are using the same dimensions together, which makes the encodings more effective.
     *     </li>
     *     <li>
     *         A hash of the dimension field values (1 byte each, up to a maximum of 4 fields).
     *         This is to cluster time series with similar values together, also helping with making encodings more effective.
     *     </li>
     *     <li>
     *         A hash of all names and values combined (16 bytes).
     *         This is to avoid hash collisions.
     *     </li>
     * </ul>
     * Note that this layout has been used with indices created before {@link IndexVersions#TSID_SINGLE_PREFIX_BYTE_FEATURE_FLAG}
     */
    private BytesRef buildMultiBytePrefixTsid() {
        throwIfNoDimensions(dimensions.size());
        Collections.sort(dimensions);

        final MurmurHash3.Hash128 scratch = new MurmurHash3.Hash128();

        // Similarity hash over every dimension name, duplicates included: this stream is not deduped.
        murmur3Hasher.reset();
        for (int i = 0; i < dimensions.size(); i++) {
            MurmurHash3.Hash128 pathHash = dimensions.get(i).pathHash();
            murmur3Hasher.addLong(pathHash.h1 ^ pathHash.h2);
        }
        final byte nameSimilarityByte = similarityByte(murmur3Hasher.digestHash(scratch));

        // Similarity byte for the first value of each distinct path, capped.
        final byte[] valueSimilarityBytes = new byte[MAX_TSID_VALUE_SIMILARITY_FIELDS];
        int emitted = 0;
        String previousPath = null;
        for (int i = 0; emitted < MAX_TSID_VALUE_SIMILARITY_FIELDS && i < dimensions.size(); i++) {
            Dimension dim = dimensions.get(i);
            String path = dim.path();
            if (path.equals(previousPath)) {
                // only add the first value for array fields
                continue;
            }
            MurmurHash3.Hash128 valueHash = dim.valueHash();
            valueSimilarityBytes[emitted++] = similarityByte(valueHash.h1, valueHash.h2, scratch);
            previousPath = path;
        }

        // Full hash over all names and values for uniqueness. Safe to reuse `scratch` here because the
        // similarity bytes above have already been reduced to bytes.
        murmur3Hasher.reset();
        for (int i = 0; i < dimensions.size(); i++) {
            Dimension dim = dimensions.get(i);
            murmur3Hasher.addLongs(dim.pathHash.h1, dim.pathHash.h2, dim.valueHash.h1, dim.valueHash.h2);
        }
        return writeMultiBytePrefixTsid(
            nameSimilarityByte,
            valueSimilarityBytes,
            emitted,
            dimensions.size(),
            murmur3Hasher.digestHash(scratch)
        );
    }

    private BytesRef buildSingleBytePrefixTsid() {
        throwIfNoDimensions(dimensions.size());
        Collections.sort(dimensions);

        // Two distinct Hash128 instances: `fullHash` is still live when the prefix byte is computed,
        // and the prefix computation overwrites its scratch.
        final MurmurHash3.Hash128 fullHash = new MurmurHash3.Hash128();
        murmur3Hasher.reset();
        for (Dimension dim : dimensions) {
            murmur3Hasher.addLongs(dim.pathHash.h1, dim.pathHash.h2, dim.valueHash.h1, dim.valueHash.h2);
        }
        murmur3Hasher.digestHash(fullHash);

        // Lowest-ranked special dimension wins; strict `<` keeps the *first* occurrence, matching the
        // historic "first dimension with this path in sorted order" lookup for array-valued fields.
        int bestRank = PREFIX_RANK_NONE;
        long bestValueH1 = 0;
        long bestValueH2 = 0;
        for (Dimension dim : dimensions) {
            int rank = prefixByteRank(dim.path());
            if (rank < bestRank) {
                bestRank = rank;
                bestValueH1 = dim.valueHash().h1;
                bestValueH2 = dim.valueHash().h2;
                if (rank == 0) {
                    break; // nothing outranks the OTel metric-names hash
                }
            }
        }

        final MurmurHash3.Hash128 scratch = new MurmurHash3.Hash128();
        MurmurHash3.Hash128 nameSimilarityHash = null;
        if (bestRank == PREFIX_RANK_NONE) {
            // Only folded when no special dimension is present, preserving the historic short-circuit.
            murmur3Hasher.reset();
            for (Dimension dim : dimensions) {
                murmur3Hasher.addLong(dim.pathHash.h1 ^ dim.pathHash.h2);
            }
            nameSimilarityHash = murmur3Hasher.digestHash(scratch);
        }
        return writeSingleBytePrefixTsid(singleBytePrefix(bestRank, bestValueH1, bestValueH2, nameSimilarityHash, scratch), fullHash);
    }

    /**
     * The sole definition of a string dimension's value hash: murmur3-128 of the UTF-8 bytes, seed 0.
     *
     * @param out output holder; callers storing the result in a longer-lived structure must pass a
     *            fresh instance rather than a reused scratch
     */
    static MurmurHash3.Hash128 hashStringValue(byte[] utf8Bytes, int offset, int length, MurmurHash3.Hash128 out) {
        return MurmurHash3.hash128(utf8Bytes, offset, length, 0L, out);
    }

    /**
     * The similarity byte derived from a single 128-bit hash: murmur3-128 (seed 0) of the eight
     * little-endian bytes of {@code h1 ^ h2}, reduced to the low byte of the resulting {@code h1}.
     *
     * <p>Sole definition of the value-similarity bytes of the multi-byte layout and of the
     * OTel / Prometheus prefix byte of the single-byte layout.
     */
    static byte similarityByte(long h1, long h2, MurmurHash3.Hash128 scratch) {
        return (byte) MurmurHash3.hashLongToH1(h1 ^ h2, scratch);
    }

    /** The similarity byte of an already-accumulated stream hash. */
    static byte similarityByte(MurmurHash3.Hash128 streamHash) {
        return (byte) streamHash.h1;
    }

    /**
     * Priority of a dimension path as the source of the single prefix byte; lower wins. Sole
     * definition of the OTel-then-Prometheus precedence.
     *
     * <p>Callers track the minimum rank over a tsid's dimensions and must replace the incumbent only
     * on a <em>strictly</em> lower rank, so that an array-valued special dimension contributes its
     * first value.
     */
    static int prefixByteRank(String path) {
        if (OTEL_METRIC_FIELD.equals(path)) {
            return 0;
        }
        if (PROMETHEUS_LABEL_FIELD.equals(path)) {
            return 1;
        }
        return PREFIX_RANK_NONE;
    }

    /**
     * Sole definition of the single prefix byte.
     *
     * @param bestRank           lowest {@link #prefixByteRank} among the tsid's dimensions, or
     *                           {@link #PREFIX_RANK_NONE} when none is special
     * @param nameSimilarityHash finalized hash over the concatenated {@code pathH1 ^ pathH2} words of
     *                           all dimensions in sorted order, duplicates included. Required exactly
     *                           when {@code bestRank} is {@link #PREFIX_RANK_NONE}, and otherwise
     *                           unused so callers can skip that fold entirely.
     */
    static byte singleBytePrefix(
        int bestRank,
        long bestValueH1,
        long bestValueH2,
        MurmurHash3.Hash128 nameSimilarityHash,
        MurmurHash3.Hash128 scratch
    ) {
        assert (bestRank == PREFIX_RANK_NONE) == (nameSimilarityHash != null)
            : "name similarity hash must be supplied exactly when no special dimension is present";
        return bestRank == PREFIX_RANK_NONE ? similarityByte(nameSimilarityHash) : similarityByte(bestValueH1, bestValueH2, scratch);
    }

    /**
     * Sole definition of the single-prefix-byte tsid layout: 16 bytes holding the full hash's
     * {@code h2} then {@code h1} little-endian, with byte 0 then overwritten by {@code prefixByte}.
     *
     * <p>The prefix byte is written last because it deliberately clobbers the least significant byte
     * of {@code h2}; reordering these two writes changes the result.
     */
    static BytesRef writeSingleBytePrefixTsid(byte prefixByte, MurmurHash3.Hash128 fullHash) {
        final byte[] tsid = new byte[FULL_HASH_BYTES];
        writeHash128(fullHash, tsid, 0);
        tsid[0] = prefixByte;
        return new BytesRef(tsid);
    }

    /**
     * Sole definition of the legacy multi-prefix-byte tsid layout: one name-similarity byte, then
     * {@code valueSimilarityByteCount} value-similarity bytes, then the 16-byte full hash.
     *
     * <p>The backing array is sized {@code 1 + min(MAX_TSID_VALUE_SIMILARITY_FIELDS, dimensionCount)
     * + 16} while the returned length is {@code 1 + valueSimilarityByteCount + 16}. These differ
     * whenever array values collapsed onto one path, so that fewer similarity bytes were emitted than
     * there was room for; the trailing bytes fall outside the returned slice. The two counts are
     * driven by different quantities (distinct paths vs total values) and must not be conflated.
     *
     * @param dimensionCount total dimension count including array elements; affects only the backing
     *                       array capacity, never the returned bytes
     */
    static BytesRef writeMultiBytePrefixTsid(
        byte nameSimilarityByte,
        byte[] valueSimilarityBytes,
        int valueSimilarityByteCount,
        int dimensionCount,
        MurmurHash3.Hash128 fullHash
    ) {
        final byte[] tsid = new byte[1 + Math.min(MAX_TSID_VALUE_SIMILARITY_FIELDS, dimensionCount) + FULL_HASH_BYTES];
        int index = 0;
        tsid[index++] = nameSimilarityByte;
        System.arraycopy(valueSimilarityBytes, 0, tsid, index, valueSimilarityByteCount);
        index += valueSimilarityByteCount;
        index = writeHash128(fullHash, tsid, index);
        return new BytesRef(tsid, 0, index);
    }

    /** Sole definition of the empty-dimensions failure, so both tsid paths throw the same message. */
    static void throwIfNoDimensions(int dimensionCount) {
        if (dimensionCount == 0) {
            throw new IllegalArgumentException("Dimensions are empty");
        }
    }

    private static int writeHash128(MurmurHash3.Hash128 hash128, byte[] buffer, int index) {
        ByteUtils.writeLongLE(hash128.h2, buffer, index);
        index += 8;
        ByteUtils.writeLongLE(hash128.h1, buffer, index);
        index += 8;
        return index;
    }

    public int size() {
        return dimensions.size();
    }

    /**
     * A functional interface that describes how objects of a complex type are added to a TSID.
     *
     * @param <T> the type of the value
     */
    @FunctionalInterface
    public interface TsidFunnel<T> {
        void add(T value, TsidBuilder tsidBuilder);
    }

    /**
     * A functional interface that describes how objects of a complex type are added to a TSID,
     * allowing for exceptions to be thrown during the process.
     *
     * @param <T> the type of the value
     * @param <E> the type of exception that can be thrown
     */
    @FunctionalInterface
    public interface ThrowingTsidFunnel<T, E extends Exception> {
        void add(T value, TsidBuilder tsidBuilder) throws E;
    }

    private record Dimension(String path, MurmurHash3.Hash128 pathHash, MurmurHash3.Hash128 valueHash, int insertionOrder)
        implements
            Comparable<Dimension> {
        @Override
        public int compareTo(Dimension o) {
            int i = path.compareTo(o.path);
            if (i != 0) return i;
            // ensures array values are in the order as they appear in the source
            return Integer.compare(insertionOrder, o.insertionOrder);
        }
    }
}
