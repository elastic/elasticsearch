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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.RoutingPathFields;
import org.elasticsearch.xcontent.XContentString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

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

    private static final int FULL_HASH_BYTES = 16;

    /** Position of the prefixed-dimensions hash in the single-prefix-byte layout: right after the prefix byte. */
    static final int PREFIXED_DIMENSIONS_HASH_OFFSET = 1;
    /** Width of the prefixed-dimensions hash in the single-prefix-byte layout. */
    static final int PREFIXED_DIMENSIONS_HASH_BYTES = 2;

    static final long LONG_VALUE_TAG = 1L;
    static final long DOUBLE_VALUE_TAG = 2L;
    static final long BOOLEAN_VALUE_TAG = 3L;

    /** {@link #prefixByteRank} of a path that is not special. */
    static final int PREFIX_RANK_NONE = Integer.MAX_VALUE;

    /** Sentinel for "no path seen yet" in the columnar path's per-row dedup cursor. */
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
        murmur3Hasher.reset();
        murmur3Hasher.update(utf8Bytes, offset, length);
        MurmurHash3.Hash128 hash128 = murmur3Hasher.digestHash();
        addDimension(path, hash128);
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

    /**
     * Builds the tsid of an index created at {@code indexVersion} with default index settings, that is without
     * {@link org.elasticsearch.index.IndexSettings#TIME_SERIES_TSID_PREFIXED_DIMENSIONS}. Production callers have the
     * {@link org.elasticsearch.cluster.metadata.IndexMetadata} and must go through {@link #buildTsid(TsidLayout)} with
     * {@link TsidLayout#forIndex} so that the setting is honored.
     */
    public final BytesRef buildTsid(IndexVersion indexVersion) {
        return buildTsid(TsidLayout.forVersion(indexVersion));
    }

    /** Builds the tsid in the layout of the index the document is indexed into. */
    public final BytesRef buildTsid(TsidLayout layout) {
        if (layout.singleBytePrefix()) {
            return buildSingleBytePrefixTsid(layout.prefixedDimensionMatcher());
        } else {
            return buildMultiBytePrefixTsid();
        }
    }

    public static boolean useSingleBytePrefixLayout(IndexVersion indexVersion) {
        return indexVersion.onOrAfter(IndexVersions.TSID_SINGLE_PREFIX_BYTE)
            || (SINGLE_PREFIX_BYTE_ENABLED && indexVersion.onOrAfter(IndexVersions.TSID_SINGLE_PREFIX_BYTE_FEATURE_FLAG));
    }

    /**
     * Whether the single-prefix-byte layout also carries the hash of the dimensions matching
     * {@link org.elasticsearch.index.IndexSettings#TIME_SERIES_TSID_PREFIXED_DIMENSIONS} in its second and third bytes. Implies
     * {@link #useSingleBytePrefixLayout}.
     */
    public static boolean usePrefixedDimensionsHash(IndexVersion indexVersion) {
        return indexVersion.onOrAfter(IndexVersions.TSID_PREFIXED_DIMENSIONS_HASH);
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
     * Note that this layout has been used with indices created before {@link IndexVersions#TSID_SINGLE_PREFIX_BYTE_FEATURE_FLAG}.
     * Indices created on or after it use {@link #buildSingleBytePrefixTsid}.
     */
    private BytesRef buildMultiBytePrefixTsid() {
        throwIfEmpty();
        Collections.sort(dimensions);

        int numberOfValues = Math.min(MAX_TSID_VALUE_SIMILARITY_FIELDS, dimensions.size());
        byte[] hash = new byte[1 + numberOfValues + 16];
        int index = 0;

        MurmurHash3.Hash128 hashBuffer = new MurmurHash3.Hash128();
        murmur3Hasher.reset();
        // similarity hash for dimension names
        for (int i = 0; i < dimensions.size(); i++) {
            Dimension dim = dimensions.get(i);
            murmur3Hasher.addLong(dim.pathHash.h1 ^ dim.pathHash.h2);
        }
        hash[index++] = similarityByte(murmur3Hasher.digestHash(hashBuffer));

        // similarity hash for dimension values
        String previousPath = null;
        for (int i = 0; index < numberOfValues + 1 && i < dimensions.size(); i++) {
            Dimension dim = dimensions.get(i);
            String path = dim.path();
            if (path.equals(previousPath)) {
                // only add the first value for array fields
                continue;
            }
            MurmurHash3.Hash128 valueHash = dim.valueHash();
            hash[index++] = similarityByte(valueHash.h1, valueHash.h2, hashBuffer);
            previousPath = path;
        }

        murmur3Hasher.reset();
        // full hash for all dimension names and values for uniqueness
        for (int i = 0; i < dimensions.size(); i++) {
            Dimension dim = dimensions.get(i);
            murmur3Hasher.addLongs(dim.pathHash.h1, dim.pathHash.h2, dim.valueHash.h1, dim.valueHash.h2);
        }
        index = writeHash128(murmur3Hasher.digestHash(hashBuffer), hash, index);
        return new BytesRef(hash, 0, index);
    }

    /**
     * Builds a 16-byte tsid whose leading bytes sort the time series of one metric, and within it of one resource, next to each
     * other, which is what makes the index sort and the prefix partitioning of the tsdb codec effective:
     * <ul>
     *     <li>
     *         Byte 0: a similarity byte of the metric name for the OTel and Prometheus schemas, or of the dimension names otherwise,
     *         see {@link #computeSingleBytePrefix}.
     *     </li>
     *     <li>
     *         Bytes 1-2: the low bytes of a hash over the names and values of the dimensions whose path matches
     *         {@code isPrefixedDimension}, so that the series of one metric are clustered by resource. Only written when
     *         {@code isPrefixedDimension} is non-null and at least one dimension matches it; otherwise, and for indices created
     *         before {@link IndexVersions#TSID_PREFIXED_DIMENSIONS_HASH}, these bytes keep the full hash and the layout is the
     *         original single-prefix-byte one.
     *     </li>
     *     <li>The remaining bytes: a hash over all dimension names and values, for uniqueness.</li>
     * </ul>
     * Bytes 1-2 are a hash, so a given metric still spreads over the partitions the codec derives from the first bytes of the
     * tsid, but no further than the number of distinct resources it is reported from.
     *
     * @param isPrefixedDimension matches the paths of the dimensions hashed into bytes 1-2, or null to leave the full hash there,
     *                            see {@link TsidLayout#prefixedDimensionMatcher()}
     */
    private BytesRef buildSingleBytePrefixTsid(@Nullable Predicate<String> isPrefixedDimension) {
        throwIfEmpty();
        Collections.sort(dimensions);

        final byte[] tsid = new byte[FULL_HASH_BYTES];
        murmur3Hasher.reset();
        MurmurHash3.Hash128 hashBuffer = new MurmurHash3.Hash128();
        // hash of all dimension names and values for uniqueness
        for (Dimension dim : dimensions) {
            murmur3Hasher.addLongs(dim.pathHash.h1, dim.pathHash.h2, dim.valueHash.h1, dim.valueHash.h2);
        }
        murmur3Hasher.digestHash(hashBuffer);
        writeHash128(hashBuffer, tsid, 0);
        // The full hash is already written, so the buffer is free to hold the prefixed-dimensions hash.
        MurmurHash3.Hash128 prefixedDimensionsHash = computePrefixedDimensionsHash(isPrefixedDimension, hashBuffer);
        if (prefixedDimensionsHash != null) {
            writePrefixedDimensionsHash(prefixedDimensionsHash, tsid);
        }
        tsid[0] = computeSingleBytePrefix(hashBuffer);
        return new BytesRef(tsid);
    }

    /**
     * Hashes the names and values of the dimensions matching {@code isPrefixedDimension}, in sorted order, or returns null when
     * there is no matcher or no dimension matches it. The dimensions must be sorted. {@code out} receives the hash.
     */
    @Nullable
    private MurmurHash3.Hash128 computePrefixedDimensionsHash(@Nullable Predicate<String> isPrefixedDimension, MurmurHash3.Hash128 out) {
        if (isPrefixedDimension == null) {
            return null;
        }
        murmur3Hasher.reset();
        int matched = 0;
        for (Dimension dim : dimensions) {
            if (isPrefixedDimension(dim.path, isPrefixedDimension)) {
                murmur3Hasher.addLongs(dim.pathHash.h1, dim.pathHash.h2, dim.valueHash.h1, dim.valueHash.h2);
                matched++;
            }
        }
        return matched == 0 ? null : murmur3Hasher.digestHash(out);
    }

    private byte computeSingleBytePrefix(MurmurHash3.Hash128 scratch) {
        murmur3Hasher.reset();
        Dimension otelMetric = findDimension(dimensions, OTEL_METRIC_FIELD);
        if (otelMetric != null) {
            return similarityByte(otelMetric.valueHash().h1, otelMetric.valueHash().h2, scratch);
        }
        Dimension prometheusLabel = findDimension(dimensions, PROMETHEUS_LABEL_FIELD);
        if (prometheusLabel != null) {
            return similarityByte(prometheusLabel.valueHash().h1, prometheusLabel.valueHash().h2, scratch);
        }
        // similarity hash for dimension names
        for (Dimension dim : dimensions) {
            murmur3Hasher.addLong(dim.pathHash.h1 ^ dim.pathHash.h2);
        }
        return similarityByte(murmur3Hasher.digestHash(scratch));
    }

    private static Dimension findDimension(List<Dimension> sortedDimensions, String name) {
        for (Dimension dim : sortedDimensions) {
            int cmp = dim.path.compareTo(name);
            if (cmp > 0) {
                return null;
            } else if (cmp == 0) {
                return dim;
            }
        }
        return null;
    }

    private void throwIfEmpty() {
        throwIfNoDimensions(dimensions.size());
    }

    /**
     * Re-hashes one 128-bit hash down to a similarity byte: murmur3-128 (seed 0) over the little-endian
     * bytes of {@code h1 ^ h2}, reduced to the low byte of the result.
     */
    static byte similarityByte(long h1, long h2, MurmurHash3.Hash128 scratch) {
        return (byte) MurmurHash3.hashLongToH1(h1 ^ h2, scratch);
    }

    /** Similarity byte of a hash already accumulated over a stream of words, which needs no re-hash. */
    static byte similarityByte(MurmurHash3.Hash128 streamHash) {
        return (byte) streamHash.h1;
    }

    /** Hashes a dimension path. {@code hasher} is borrowed and reset before use. */
    static MurmurHash3.Hash128 hashPath(BufferedMurmur3Hasher hasher, String path) {
        hasher.reset();
        hasher.addString(path);
        return hasher.digestHash();
    }

    static void throwIfNoDimensions(int dimensionCount) {
        if (dimensionCount == 0) {
            throw new IllegalArgumentException("Dimensions are empty");
        }
    }

    // Below: used only by the column-major path (ColumnarTsidCalculator / ColumnarTsidAccumulator).
    // Each restates a rule the code above expresses in a shape a column-major scan cannot call, so the
    // two are kept in agreement by tests comparing their bytes rather than by sharing code.

    /**
     * Adds a dimension from already-computed hashes. <b>Test-only</b>: it drives this builder from the
     * same tuples {@link ColumnarTsidAccumulator} consumes, so a parity failure points at the
     * accumulator rather than at the hashing. {@code path} is still used for the prefix-byte
     * special-case and the array-dedup guard, so pass its {@link #hashPath}.
     */
    TsidBuilder addPrehashedDimension(String path, long pathH1, long pathH2, long valueH1, long valueH2) {
        dimensions.add(
            new Dimension(path, new MurmurHash3.Hash128(pathH1, pathH2), new MurmurHash3.Hash128(valueH1, valueH2), dimensions.size())
        );
        return this;
    }

    /**
     * Value hash of a string dimension: murmur3-128 of its UTF-8 bytes, seed 0.
     *
     * @param out output holder; callers retaining the result must pass a fresh instance, not a scratch
     */
    static MurmurHash3.Hash128 hashStringValue(byte[] utf8Bytes, int offset, int length, MurmurHash3.Hash128 out) {
        return MurmurHash3.hash128(utf8Bytes, offset, length, 0L, out);
    }

    /**
     * Priority of a path as the source of the single prefix byte; lower wins. Callers must take the
     * minimum over a row's dimensions on a <em>strictly</em> lower rank, so an array-valued special
     * dimension contributes its first value — as {@link #findDimension} does for the row path.
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
     * The single prefix byte, given the winning {@link #prefixByteRank} for a row.
     *
     * <p>The two paths are asymmetric by design. When no special dimension is present,
     * {@code nameSimilarityHash} is read directly ({@code h1} as-is, no re-hash), so the caller must
     * supply a finalized hash — the output of {@link BufferedMurmur3Hasher#digestHash}. When a special
     * field is present, {@code bestValueH1}/{@code bestValueH2} are re-hashed internally via
     * {@link MurmurHash3#hashLongToH1}, using {@code scratch} as a temporary buffer.
     *
     * @param nameSimilarityHash finalized hash over the {@code pathH1 ^ pathH2} words of the row's
     *                           dimensions in sorted order, duplicates included. Needed only when no
     *                           dimension is special, so callers can otherwise skip that fold.
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
     * Whether a dimension at {@code path} contributes to the prefixed-dimensions hash of a layout whose
     * {@link TsidLayout#prefixedDimensionMatcher()} is {@code isPrefixedDimension}. Order-independent, so the columnar path
     * resolves it once per column.
     */
    static boolean isPrefixedDimension(String path, @Nullable Predicate<String> isPrefixedDimension) {
        return isPrefixedDimension != null && isPrefixedDimension.test(path);
    }

    /**
     * The single-prefix-byte layout, mirroring the tail of {@link #buildSingleBytePrefixTsid}. The
     * prefix bytes are written last because they deliberately clobber the low bytes of {@code h2}.
     *
     * @param prefixedDimensionsHash hash over the row's dimensions matching the layout's prefixed dimensions, folded like the
     *                               full hash, or null when the layout has none or the row has no matching dimension
     */
    static BytesRef writeSingleBytePrefixTsid(
        byte prefixByte,
        @Nullable MurmurHash3.Hash128 prefixedDimensionsHash,
        MurmurHash3.Hash128 fullHash
    ) {
        final byte[] tsid = new byte[FULL_HASH_BYTES];
        writeHash128(fullHash, tsid, 0);
        if (prefixedDimensionsHash != null) {
            writePrefixedDimensionsHash(prefixedDimensionsHash, tsid);
        }
        tsid[0] = prefixByte;
        return new BytesRef(tsid);
    }

    /** Writes the low {@link #PREFIXED_DIMENSIONS_HASH_BYTES} bytes of {@code h1}, little-endian, at the hash's position in the tsid. */
    private static void writePrefixedDimensionsHash(MurmurHash3.Hash128 prefixedDimensionsHash, byte[] tsid) {
        long h1 = prefixedDimensionsHash.h1;
        for (int i = 0; i < PREFIXED_DIMENSIONS_HASH_BYTES; i++) {
            tsid[PREFIXED_DIMENSIONS_HASH_OFFSET + i] = (byte) (h1 >>> (Byte.SIZE * i));
        }
    }

    /**
     * The legacy multi-prefix-byte layout: a name-similarity byte, {@code valueSimilarityByteCount}
     * value-similarity bytes, then the 16-byte full hash. Equivalent to the array
     * {@link #buildMultiBytePrefixTsid} fills in place, except that this sizes exactly — that one is
     * sized for the worst case because it does not know the final byte count until it has written them.
     */
    static BytesRef writeMultiBytePrefixTsid(
        byte nameSimilarityByte,
        byte[] valueSimilarityBytes,
        int valueSimilarityByteCount,
        MurmurHash3.Hash128 fullHash
    ) {
        assert valueSimilarityByteCount <= MAX_TSID_VALUE_SIMILARITY_FIELDS : valueSimilarityByteCount;
        assert valueSimilarityByteCount <= valueSimilarityBytes.length : valueSimilarityByteCount;
        final byte[] tsid = new byte[1 + valueSimilarityByteCount + FULL_HASH_BYTES];
        int index = 0;
        tsid[index++] = nameSimilarityByte;
        System.arraycopy(valueSimilarityBytes, 0, tsid, index, valueSimilarityByteCount);
        index += valueSimilarityByteCount;
        writeHash128(fullHash, tsid, index);
        return new BytesRef(tsid);
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
