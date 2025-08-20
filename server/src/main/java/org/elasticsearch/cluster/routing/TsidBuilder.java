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

    private static final int MAX_TSID_VALUE_FIELDS = 16;
    /**
     * The size of the buffer used for holding the UTF-8 encoded strings before passing them to the hasher.
     * This is primarily used for paths/keys as string dimension values are passed in as UTF-8 encoded bytes for the most part.
     * Should be sized so that it can hold the longest UTF-8 encoded string that is expected to be used as a dimension key,
     * to avoid re-sizing the buffer.
     * But should also be small enough to not waste memory in case the keys are short.
     */
    private static final int HASH_BUFFER_SIZE = 32 * 4; // 32 characters, each character can take up to 4 bytes in UTF-8
    private final BufferedMurmur3Hasher murmur3Hasher = new BufferedMurmur3Hasher(0L, HASH_BUFFER_SIZE);

    private final List<Dimension> dimensions = new ArrayList<>();

    /**
     * Adds an integer dimension to the TSID.
     *
     * @param path  the path of the dimension
     * @param value the integer value of the dimension
     */
    public void addIntDimension(String path, int value) {
        addDimension(path, new MurmurHash3.Hash128(1, value));
    }

    /**
     * Adds a long dimension to the TSID.
     *
     * @param path  the path of the dimension
     * @param value the long value of the dimension
     */
    public void addLongDimension(String path, long value) {
        addDimension(path, new MurmurHash3.Hash128(1, value));
    }

    /**
     * Adds a double dimension to the TSID.
     *
     * @param path  the path of the dimension
     * @param value the double value of the dimension
     */
    public void addDoubleDimension(String path, double value) {
        addDimension(path, new MurmurHash3.Hash128(2, Double.doubleToLongBits(value)));
    }

    /**
     * Adds a boolean dimension to the TSID.
     *
     * @param path  the path of the dimension
     * @param value the boolean value of the dimension
     */
    public void addBooleanDimension(String path, boolean value) {
        addDimension(path, new MurmurHash3.Hash128(3, value ? 1 : 0));
    }

    /**
     * Adds a string dimension to the TSID.
     *
     * @param path  the path of the dimension
     * @param value the string value of the dimension
     */
    public void addStringDimension(String path, String value) {
        addStringDimension(path, new BytesRef(value));
    }

    private void addStringDimension(String path, BytesRef value) {
        addStringDimension(path, value.bytes, value.offset, value.length);
    }

    /**
     * Adds a string dimension to the TSID.
     *
     * @param path  the path of the dimension
     * @param value the UTF8Bytes value of the dimension
     */
    public void addStringDimension(String path, XContentString.UTF8Bytes value) {
        addStringDimension(path, value.bytes(), value.offset(), value.length());
    }

    /**
     * Adds a string dimension to the TSID using a byte array.
     * The value is provided as UTF-8 encoded bytes[].
     *
     * @param path the path of the dimension
     * @param utf8Bytes the UTF-8 encoded bytes of the string value
     * @param offset the offset in the byte array where the string starts
     * @param length the length of the string in bytes
     */
    public void addStringDimension(String path, byte[] utf8Bytes, int offset, int length) {
        murmur3Hasher.reset();
        murmur3Hasher.update(utf8Bytes, offset, length);
        MurmurHash3.Hash128 hash128 = murmur3Hasher.digestHash();
        addDimension(path, hash128);
    }

    /**
     * Adds a value to the TSID using a funnel.
     * This allows for complex types to be added to the TSID.
     *
     * @param value  the value to add
     * @param funnel the funnel that describes how to add the value
     * @param <T>    the type of the value
     */
    public <T> void add(T value, TsidFunnel<T> funnel) {
        funnel.add(value, this);
    }

    /**
     * Adds a value to the TSID using a funnel that can throw exceptions.
     * This allows for complex types to be added to the TSID.
     *
     * @param value  the value to add
     * @param funnel the funnel that describes how to add the value
     * @param <T>    the type of the value
     * @param <E>    the type of exception that can be thrown
     * @throws E if an exception occurs while adding the value
     */
    public <T, E extends Exception> void add(T value, ThrowingTsidFunnel<T, E> funnel) throws E {
        funnel.add(value, this);
    }

    private void addDimension(String path, MurmurHash3.Hash128 valueHash) {
        murmur3Hasher.reset();
        murmur3Hasher.addString(path);
        MurmurHash3.Hash128 pathHash = murmur3Hasher.digestHash();
        dimensions.add(new Dimension(path, pathHash, valueHash, dimensions.size()));
    }

    /**
     * Adds all dimensions from another TsidBuilder to this one.
     * If the other builder is null or has no dimensions, this method does nothing.
     *
     * @param other the other TsidBuilder to add dimensions from
     */
    public void addAll(TsidBuilder other) {
        if (other == null || other.dimensions.isEmpty()) {
            return;
        }
        dimensions.addAll(other.dimensions);
    }

    /**
     * Computes the hash of the dimensions added to this builder.
     * The hash is a 128-bit value that is computed based on the dimension names and values.
     *
     * @return a HashValue128 representing the hash of the dimensions
     * @throws IllegalArgumentException if no dimensions have been added
     */
    public MurmurHash3.Hash128 hash() {
        if (dimensions.isEmpty()) {
            throw new IllegalArgumentException("Error extracting dimensions: no dimension fields found");
        }
        Collections.sort(dimensions);
        murmur3Hasher.reset();
        for (Dimension dim : dimensions) {
            murmur3Hasher.addLongs(dim.pathHash.h1, dim.pathHash.h2, dim.valueHash.h1, dim.valueHash.h2);
        }
        return murmur3Hasher.digestHash();
    }

    /**
     * Builds a time series identifier (TSID) based on the dimensions added to this builder.
     * This is a slight adaptation of {@link RoutingPathFields#buildHash()} but creates shorter tsids.
     * The TSID is a hash that includes:
     * <ul>
     *     <li>
     *         A hash of the dimension field names (4 bytes).
     *         This is to cluster time series that are using the same dimensions together, which makes the encodings more effective.
     *     </li>
     *     <li>
     *         A hash of the dimension field values (1 byte each, up to a maximum of 16 fields).
     *         This is to cluster time series with similar values together, also helping with making encodings more effective.
     *     </li>
     *     <li>
     *         A hash of all names and values combined (16 bytes).
     *         This is to avoid hash collisions.
     *     </li>
     * </ul>
     *
     * @return a BytesRef containing the TSID
     * @throws IllegalArgumentException if no dimensions have been added
     */
    public BytesRef buildTsid() {
        if (dimensions.isEmpty()) {
            throw new IllegalArgumentException("Error extracting dimensions: source didn't contain any routing fields");
        }

        int numberOfValues = Math.min(MAX_TSID_VALUE_FIELDS, dimensions.size());
        byte[] hash = new byte[4 + numberOfValues + 16];
        int index = 0;

        Collections.sort(dimensions);

        MurmurHash3.Hash128 hashBuffer = new MurmurHash3.Hash128();
        murmur3Hasher.reset();
        for (int i = 0; i < dimensions.size(); i++) {
            Dimension dim = dimensions.get(i);
            murmur3Hasher.addLongs(dim.pathHash.h1, dim.pathHash.h2);
        }
        ByteUtils.writeIntLE((int) murmur3Hasher.digestHash(hashBuffer).h1, hash, index);
        index += 4;

        // similarity hash for values
        String previousPath = null;
        for (int i = 0; i < numberOfValues; i++) {
            Dimension dim = dimensions.get(i);
            String path = dim.path();
            if (path.equals(previousPath)) {
                // only add the first value for array fields
                continue;
            }
            MurmurHash3.Hash128 valueHash = dim.valueHash();
            murmur3Hasher.reset();
            murmur3Hasher.addLongs(valueHash.h1, valueHash.h2);
            hash[index++] = (byte) murmur3Hasher.digestHash(hashBuffer).h1;
            previousPath = path;
        }

        murmur3Hasher.reset();
        for (int i = 0; i < dimensions.size(); i++) {
            Dimension dim = dimensions.get(i);
            murmur3Hasher.addLongs(dim.pathHash.h1, dim.pathHash.h2, dim.valueHash.h1, dim.valueHash.h2);
        }
        index = writeHash128(murmur3Hasher.digestHash(hashBuffer), hash, index);
        return new BytesRef(hash, 0, index);
    }

    private static int writeHash128(MurmurHash3.Hash128 hash128, byte[] buffer, int index) {
        ByteUtils.writeLongLE(hash128.h2, buffer, index);
        index += 8;
        ByteUtils.writeLongLE(hash128.h1, buffer, index);
        index += 8;
        return index;
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
