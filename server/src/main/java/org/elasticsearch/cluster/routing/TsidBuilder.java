/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import com.dynatrace.hash4j.hashing.HashStream128;
import com.dynatrace.hash4j.hashing.HashStream32;
import com.dynatrace.hash4j.hashing.HashValue128;
import com.dynatrace.hash4j.hashing.Hasher128;
import com.dynatrace.hash4j.hashing.Hasher32;
import com.dynatrace.hash4j.hashing.Hashing;

import org.apache.lucene.util.BytesRef;
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
    private static final Hasher128 HASHER_128 = Hashing.murmur3_128();
    private static final Hasher32 HASHER_32 = Hashing.murmur3_32();
    private final HashStream128 hashStream = HASHER_128.hashStream();

    private final List<Dimension> dimensions;

    public TsidBuilder() {
        dimensions = new ArrayList<>();
    }

    public TsidBuilder(int expectedSize) {
        dimensions = new ArrayList<>(expectedSize);
    }

    /**
     * Adds an integer dimension to the TSID.
     *
     * @param path  the path of the dimension
     * @param value the integer value of the dimension
     */
    public void addIntDimension(String path, int value) {
        addDimension(path, new HashValue128(1, value));
    }

    /**
     * Adds a long dimension to the TSID.
     *
     * @param path  the path of the dimension
     * @param value the long value of the dimension
     */
    public void addLongDimension(String path, long value) {
        addDimension(path, new HashValue128(1, value));
    }

    /**
     * Adds a double dimension to the TSID.
     *
     * @param path  the path of the dimension
     * @param value the double value of the dimension
     */
    public void addDoubleDimension(String path, double value) {
        addDimension(path, new HashValue128(2, Double.doubleToLongBits(value)));
    }

    /**
     * Adds a boolean dimension to the TSID.
     *
     * @param path  the path of the dimension
     * @param value the boolean value of the dimension
     */
    public void addBooleanDimension(String path, boolean value) {
        addDimension(path, new HashValue128(3, value ? 1 : 0));
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
        hashStream.reset();
        hashStream.putBytes(utf8Bytes, offset, length);
        HashValue128 valueHash = hashStream.get();
        addDimension(path, valueHash);
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

    private void addDimension(String path, HashValue128 valueHash) {
        hashStream.reset();
        hashStream.putString(path);
        HashValue128 pathHash = hashStream.get();
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
    public HashValue128 hash() {
        return hashStream().get();
    }

    public HashStream128 hashStream() {
        if (dimensions.isEmpty()) {
            throw new IllegalArgumentException("Error extracting dimensions: no dimension fields found");
        }
        Collections.sort(dimensions);
        HashStream128 hashStream = HASHER_128.hashStream();
        for (Dimension dim : dimensions) {
            hashStream.putLong(dim.pathHash.getMostSignificantBits());
            hashStream.putLong(dim.pathHash.getLeastSignificantBits());
            hashStream.putLong(dim.valueHash.getMostSignificantBits());
            hashStream.putLong(dim.valueHash.getLeastSignificantBits());
        }
        return hashStream;
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
        HashStream32 fieldNameHash = HASHER_32.hashStream();
        HashStream128 dimensionsHash = HASHER_128.hashStream();
        for (int i = 0; i < dimensions.size(); i++) {
            Dimension dim = dimensions.get(i);
            fieldNameHash.putLong(dim.pathHash.getMostSignificantBits());
            fieldNameHash.putLong(dim.pathHash.getLeastSignificantBits());
            dimensionsHash.putLong(dim.pathHash.getMostSignificantBits());
            dimensionsHash.putLong(dim.pathHash.getLeastSignificantBits());
            dimensionsHash.putLong(dim.valueHash.getMostSignificantBits());
            dimensionsHash.putLong(dim.valueHash.getLeastSignificantBits());
        }
        ByteUtils.writeIntLE(fieldNameHash.getAsInt(), hash, index);
        index += 4;

        // similarity hash for values
        String previousPath = null;
        for (int i = 0; i < numberOfValues; i++) {
            Dimension dim = dimensions.get(i);
            String path = dim.path();
            if (path == previousPath) {
                // only add the first value for array fields
                continue;
            }
            hash[index++] = (byte) dim.valueHash().getAsInt();
            previousPath = path;
        }

        index = writeHash128(dimensionsHash.get(), hash, index);
        return new BytesRef(hash, 0, index);
    }

    private static int writeHash128(HashValue128 hash128, byte[] buffer, int index) {
        ByteUtils.writeLongLE(hash128.getLeastSignificantBits(), buffer, index);
        index += 8;
        ByteUtils.writeLongLE(hash128.getMostSignificantBits(), buffer, index);
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

    private record Dimension(String path, HashValue128 pathHash, HashValue128 valueHash, int insertionOrder)
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
