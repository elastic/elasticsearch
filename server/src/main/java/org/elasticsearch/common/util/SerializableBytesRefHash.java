/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.hppc.BitMixer;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.Locale;

/**
 * Serializable version of {@link BytesRefHash}.
 *
 * Differences to {@link BytesRefHash}:
 *
 * - {@link BytesRefHash} rehash function uses a different seed for every JVM invocation,
 *   that means the hash functions returns different values
 * - SerializableBytesRefHash uses random seed, but serializes the seed
 * - the content of used big arrays are cleared, because serialization uses variable length encodings
 */
public class SerializableBytesRefHash extends AbstractBytesRefHash implements Writeable {

    /**
     * Encoding types for serializing the BytesRefHash
     *
     * Currently encodes the hash table by simply dumping all its internal data structures.
     * TODO: This might not be the smartest way to serialize and deserialize a sparse structure,
     * at least it uses variable length encodings to save some space.
     */
    public enum SerializationType {
        RAW_BUFFERS(0);

        private final byte type;

        SerializationType(int type) {
            this.type = (byte) type;
        }

        public byte getType() {
            return type;
        }

        public static SerializationType fromType(byte id) {
            return switch (id) {
                case 0 -> RAW_BUFFERS;
                default -> throw new IllegalArgumentException("unknown type");
            };
        }

        public String value() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private final int seed;

    // Constructor with configurable capacity and default maximum load factor.
    public SerializableBytesRefHash(long capacity, BigArrays bigArrays) {
        this(capacity, DEFAULT_MAX_LOAD_FACTOR, bigArrays);
    }

    // Constructor with configurable capacity and load factor.
    public SerializableBytesRefHash(long capacity, float maxLoadFactor, BigArrays bigArrays) {
        super(capacity, maxLoadFactor, true, bigArrays);
        this.seed = Randomness.get().nextInt();
    }

    // private constructor used by loadFromStream
    private SerializableBytesRefHash(
        long capacity,
        float maxLoadFactor,
        long size,
        long maxSize,
        LongArray ids,
        LongArray startOffsets,
        ByteArray bytes,
        IntArray hashes,
        int seed,
        BigArrays bigArrays
    ) {
        super(capacity, maxLoadFactor, size, maxSize, ids, startOffsets, bytes, hashes, bigArrays);
        this.seed = seed;
    }

    public static SerializableBytesRefHash loadFromStream(StreamInput in, BigArrays bigArrays) throws IOException {
        float maxLoadFactor = in.readFloat();
        long size = in.readVLong();
        long maxSize = in.readVLong();
        long capacity = in.readLong();

        LongArray ids = null;
        LongArray startOffsets = null;
        ByteArray bytes = null;
        IntArray hashes = null;
        SerializableBytesRefHash hash = null;

        // we allocate big arrays so we have to `close` if we fail here or we'll leak them.
        boolean success = false;

        try {
            ids = bigArrays.newLongArray(capacity, true);
            for (int i = 0; i < capacity; ++i) {
                ids.set(i, in.readVLong());
            }

            // there is just 1 way of serializing at the moment
            SerializationType serializationType = SerializationType.fromType(in.readByte());
            if (SerializationType.RAW_BUFFERS.equals(serializationType) == false) {
                throw new IllegalArgumentException("unsupported type");
            }

            // startOffsets
            long sizeOfStartOffsets = in.readVLong();
            startOffsets = bigArrays.newLongArray(sizeOfStartOffsets, true);
            for (int i = 0; i < sizeOfStartOffsets; ++i) {
                startOffsets.set(i, in.readVLong());
            }

            // bytes
            long sizeOfBytes = in.readVLong();
            bytes = bigArrays.newByteArray(sizeOfBytes, true);

            for (int i = 0; i < sizeOfBytes; ++i) {
                bytes.set(i, in.readByte());
            }

            // hashes
            long sizeOfHashes = in.readVLong();
            hashes = bigArrays.newIntArray(sizeOfHashes, true);
            for (int i = 0; i < sizeOfHashes; ++i) {
                hashes.set(i, in.readZInt());
            }

            int seed = in.readInt();
            hash = new SerializableBytesRefHash(capacity, maxLoadFactor, size, maxSize, ids, startOffsets, bytes, hashes, seed, bigArrays);
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(ids, startOffsets, bytes, hashes);
            }
        }

        return hash;
    }

    @Override
    public long find(BytesRef key) {
        return find(key, hash(key));
    }

    @Override
    public long add(BytesRef key) {
        return add(key, hash(key));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeFloat(maxLoadFactor);
        out.writeVLong(size);
        out.writeVLong(maxSize);

        long capacity = capacity();
        out.writeLong(capacity);

        for (int i = 0; i < capacity; ++i) {
            out.writeVLong(ids.get(i));
        }

        out.write(SerializationType.RAW_BUFFERS.getType());

        long sizeOfStartOffsets = startOffsets.size();
        long sizeOfBytes = bytes.size();
        long sizeOfHashes = hashes.size();

        // startOffsets
        out.writeVLong(sizeOfStartOffsets);
        for (int i = 0; i < sizeOfStartOffsets; ++i) {
            out.writeVLong(startOffsets.get(i));
        }

        // bytes
        out.writeVLong(sizeOfBytes);
        for (int i = 0; i < sizeOfBytes; ++i) {
            out.writeByte(bytes.get(i));
        }

        // hashes
        out.writeVLong(sizeOfHashes);

        for (int i = 0; i < sizeOfHashes; ++i) {
            out.writeZInt(hashes.get(i));
        }

        // write the random seed
        out.writeInt(seed);
    }

    private int hash(BytesRef key) {
        // BytesRef.hashCode() uses a seed that changes per JVM invocation, as we need a stable seed we use our
        return BitMixer.mix32(StringHelper.murmurhash3_x86_32(key, seed));
    }
}
