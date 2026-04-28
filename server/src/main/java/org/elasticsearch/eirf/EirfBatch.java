/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Releasable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Immutable reader for an EIRF (Elastic Internal Row Format) batch.
 *
 * <p>Binary layout (32-byte header, all multi-byte integers little-endian):
 * <pre>
 * magic(4) version(i32) flags(i32) doc_count(i32) schema_offset(i32) doc_index_offset(i32) data_offset(i32) total_size(i32)
 * [Schema] non_leaf_count(u16) + entries + leaf_count(u16) + entries
 * [Doc Index] entries[doc_count]: data_offset(i32) + data_length(i32)
 * [Row Data] rows
 * </pre>
 */
public final class EirfBatch implements Releasable, Accountable {

    /** Magic as a little-endian int: bytes 'e','i','r','f' read as LE i32. */
    public static final int MAGIC_LE = ('e' & 0xFF) | (('i' & 0xFF) << 8) | (('r' & 0xFF) << 16) | (('f' & 0xFF) << 24);
    public static final int VERSION = 1;

    private final BytesReference data;
    private final Releasable releasable;
    private final int docCount;
    private final EirfSchema schema;
    private final int docIndexOffset;
    private final int dataOffset;

    public EirfBatch(BytesReference data, Releasable releasable) {
        this.data = data;
        this.releasable = releasable;

        int magic = data.getIntLE(0);
        if (magic != MAGIC_LE) {
            throw new IllegalArgumentException(
                "Invalid magic: expected 'eirf', got '"
                    + (char) (magic & 0xFF)
                    + (char) ((magic >> 8) & 0xFF)
                    + (char) ((magic >> 16) & 0xFF)
                    + (char) ((magic >> 24) & 0xFF)
                    + "'"
            );
        }
        int version = data.getIntLE(4);
        if (version != VERSION) {
            throw new IllegalArgumentException("Unsupported version: " + version);
        }
        this.docCount = data.getIntLE(12);
        int schemaOffset = data.getIntLE(16);
        this.docIndexOffset = data.getIntLE(20);
        this.dataOffset = data.getIntLE(24);

        this.schema = parseSchema(data, schemaOffset);
    }

    private static EirfSchema parseSchema(BytesReference data, int offset) {
        // Non-leaf fields (all u16 LE)
        int nonLeafCount = readU16LE(data, offset);
        offset += 2;
        List<String> nonLeafNames = new ArrayList<>(nonLeafCount);
        int[] nonLeafParents = new int[nonLeafCount];
        for (int i = 0; i < nonLeafCount; i++) {
            nonLeafParents[i] = readU16LE(data, offset);
            offset += 2;
            int nameLen = readU16LE(data, offset);
            offset += 2;
            if (nameLen > 0) {
                BytesRef bytesRef = data.slice(offset, nameLen).toBytesRef();
                nonLeafNames.add(new String(bytesRef.bytes, bytesRef.offset, bytesRef.length, StandardCharsets.UTF_8));
            } else {
                nonLeafNames.add("");
            }
            offset += nameLen;
        }

        // Leaf fields (all u16 LE)
        int leafCount = readU16LE(data, offset);
        offset += 2;
        List<String> leafNames = new ArrayList<>(leafCount);
        int[] leafParents = new int[leafCount];
        for (int i = 0; i < leafCount; i++) {
            leafParents[i] = readU16LE(data, offset);
            offset += 2;
            int nameLen = readU16LE(data, offset);
            offset += 2;
            BytesRef bytesRef = data.slice(offset, nameLen).toBytesRef();
            leafNames.add(new String(bytesRef.bytes, bytesRef.offset, bytesRef.length, StandardCharsets.UTF_8));
            offset += nameLen;
        }

        return new EirfSchema(nonLeafNames, nonLeafParents, leafNames, leafParents);
    }

    // TODO: Move directly to bytes reference
    static int readU16LE(BytesReference data, int offset) {
        return (data.get(offset) & 0xFF) | ((data.get(offset + 1) & 0xFF) << 8);
    }

    public int docCount() {
        return docCount;
    }

    public EirfSchema schema() {
        return schema;
    }

    public BytesReference data() {
        return data;
    }

    public int columnCount() {
        return schema.leafCount();
    }

    public EirfRowReader getRowReader(int docIndex) {
        if (docIndex < 0 || docIndex >= docCount) {
            throw new IndexOutOfBoundsException("docIndex " + docIndex + " out of range [0, " + docCount + ")");
        }
        int entryOffset = docIndexOffset + docIndex * 8;
        int rowDataOffset = data.getIntLE(entryOffset);
        int rowDataLength = data.getIntLE(entryOffset + 4);
        return new EirfRowReader(data.slice(dataOffset + rowDataOffset, rowDataLength), schema);
    }

    @Override
    public void close() {
        releasable.close();
    }

    @Override
    public long ramBytesUsed() {
        return data.length() + 64;
    }
}
