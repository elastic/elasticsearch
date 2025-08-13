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

public class TsidBuilder {

    private static final int MAX_TSID_VALUE_FIELDS = 16;
    private static final Hasher128 HASHER_128 = Hashing.murmur3_128();
    private static final Hasher32 HASHER_32 = Hashing.murmur3_32();
    private final HashStream128 hashStream = HASHER_128.hashStream();

    private final List<Dimension> dimensions = new ArrayList<>();

    public void addIntDimension(String path, int value) {
        addDimension(path, new HashValue128(1, value));
    }

    public void addLongDimension(String path, long value) {
        addDimension(path, new HashValue128(2, value));
    }

    public void addDoubleDimension(String path, double value) {
        addDimension(path, new HashValue128(2, Double.doubleToLongBits(value)));
    }

    public void addBooleanDimension(String path, boolean value) {
        addDimension(path, new HashValue128(4, value ? 1 : 0));
    }

    public void addStringDimension(String path, String value) {
        addStringDimension(path, new BytesRef(value));
    }

    private void addStringDimension(String path, BytesRef value) {
        addStringDimension(path, value.bytes, value.offset, value.length);
    }

    public void addStringDimension(String path, XContentString.UTF8Bytes value) {
        addStringDimension(path, value.bytes(), value.offset(), value.length());
    }

    public void addStringDimension(String path, byte[] value) {
        addStringDimension(path, value, 0, value.length);
    }

    private void addStringDimension(String path, byte[] bytes, int offset, int length) {
        hashStream.reset();
        hashStream.putBytes(bytes, offset, length);
        HashValue128 valueHash = hashStream.get();
        addDimension(path, valueHash);
    }

    public void addBytesDimension(String path, byte[] value) {
        hashStream.reset();
        hashStream.putBytes(value);
        HashValue128 valueHash = hashStream.get();
        addDimension(path, valueHash);
    }

    public <T> void add(T value, TsidFunnel<T> funnel) {
        funnel.add(value, this);
    }

    public <T, E extends Exception> void add(T value, ThrowingTsidFunnel<T, E> funnel) throws E {
        funnel.add(value, this);
    }

    private void addDimension(String path, HashValue128 valueHash) {
        hashStream.reset();
        hashStream.putString(path);
        HashValue128 pathHash = hashStream.get();
        dimensions.add(new Dimension(path, pathHash, valueHash, dimensions.size()));
    }

    public void addAll(TsidBuilder other) {
        if (other == null || other.dimensions.isEmpty()) {
            return;
        }
        dimensions.addAll(other.dimensions);
    }

    public HashValue128 hash() {
        if (dimensions.isEmpty()) {
            throw new IllegalArgumentException("Error extracting routing: source didn't contain any routing fields");
        }
        Collections.sort(dimensions);
        HashStream128 hashStream = HASHER_128.hashStream();
        for (Dimension dim : dimensions) {
            hashStream.putLong(dim.pathHash.getMostSignificantBits());
            hashStream.putLong(dim.pathHash.getLeastSignificantBits());
            hashStream.putLong(dim.value.getMostSignificantBits());
            hashStream.putLong(dim.value.getLeastSignificantBits());
        }
        return hashStream.get();
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
            dimensionsHash.putLong(dim.value.getMostSignificantBits());
            dimensionsHash.putLong(dim.value.getLeastSignificantBits());
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
            hash[index++] = (byte) dim.value().getAsInt();
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

    public interface TsidFunnel<T> {
        void add(T value, TsidBuilder tsidBuilder);
    }

    public interface ThrowingTsidFunnel<T, E extends Exception> {
        void add(T value, TsidBuilder tsidBuilder) throws E;
    }

    private record Dimension(String path, HashValue128 pathHash, HashValue128 value, int order) implements Comparable<Dimension> {
        @Override
        public int compareTo(Dimension o) {
            int i = path.compareTo(o.path);
            if (i != 0) return i;
            // ensures array values are in the order as they appear in the source
            return Integer.compare(order, o.order);
        }
    }
}
