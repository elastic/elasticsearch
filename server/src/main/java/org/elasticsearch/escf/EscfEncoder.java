/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.sourcebatch.LeafSink;
import org.elasticsearch.sourcebatch.SourceBatchEncodeHelper;
import org.elasticsearch.sourcebatch.SourceBatchEncoder;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Encodes XContentType documents into {@link EscfBatch}es (Elasticsearch Column Format), accumulating one
 * column per leaf field. Numbers upcast aggressively (JSON int/long → {@code long}, float/double →
 * {@code double}); a type conflict or an explicit null promotes the column to
 * {@link EscfColumnKind#UNION}. Fixed primitive arrays are stored in a columnar list layout;
 * other arrays (heterogeneous, nested, object-bearing) are stored inline on a union column.
 *
 * <p>Implements {@link SourceBatchEncoder}. Single-partition convenience: {@link #encode(List, XContentType)}.
 */
public final class EscfEncoder implements SourceBatchEncoder {

    private static final int INITIAL_CAPACITY = 16;
    private static final int INITIAL_PARTITION_CAPACITY = 4;

    private final SourceSchema schema;
    private final Recycler<BytesRef> recycler;
    private Partition[] partitions;

    private byte[] scratchType;
    private long[] scratchNumeric;
    private Object[] scratchVar;
    private FixedBitSet columnsSet;
    private boolean rowStaged;
    private String[] cachedPath;

    public EscfEncoder() {
        this(BytesRefRecycler.NON_RECYCLING_INSTANCE);
    }

    public EscfEncoder(Recycler<BytesRef> recycler) {
        this.recycler = recycler;
        this.schema = new SourceSchema();
        this.partitions = new Partition[INITIAL_PARTITION_CAPACITY];
        this.scratchType = new byte[INITIAL_CAPACITY];
        this.scratchNumeric = new long[INITIAL_CAPACITY];
        this.scratchVar = new Object[INITIAL_CAPACITY];
        this.columnsSet = new FixedBitSet(Math.max(INITIAL_CAPACITY, 64));
        this.cachedPath = new String[INITIAL_CAPACITY];
    }

    @Override
    public void parseToScratch(BytesReference source, XContentType xContentType, LeafSink sink) throws IOException {
        int columnCountBefore = schema.leafCount();
        Arrays.fill(scratchType, 0, Math.min(columnCountBefore, scratchType.length), (byte) 0);
        Arrays.fill(scratchVar, 0, Math.min(columnCountBefore, scratchVar.length), null);
        columnsSet.clear();
        try (XContentParser parser = XContentHelper.createParserNotCompressed(XContentParserConfiguration.EMPTY, source, xContentType)) {
            parser.allowDuplicateKeys(true);
            parser.nextToken(); // START_OBJECT
            flattenObject(parser, 0, parser.nextToken(), sink);
        }
        rowStaged = true;
    }

    @Override
    public int commitScratchTo(int partitionKey) {
        if (rowStaged == false) {
            throw new IllegalStateException("commitScratchTo called without a staged row");
        }
        final Partition partition = getOrCreatePartition(partitionKey);
        final int leafCount = schema.leafCount();
        ensurePartitionBuilders(partition, leafCount);
        for (int c = 0; c < leafCount; c++) {
            appendScratchValue(partition.builders.get(c), c);
        }
        final int rowIndex = partition.docCount;
        partition.docCount++;
        rowStaged = false;
        return rowIndex;
    }

    @Override
    public EscfBatch buildPartition(int partitionKey) {
        final Partition partition = getOrCreatePartition(partitionKey);
        final int leafCount = schema.leafCount();
        ensurePartitionBuilders(partition, leafCount);
        final EscfColumnData[] columns = new EscfColumnData[leafCount];
        for (int c = 0; c < leafCount; c++) {
            columns[c] = partition.builders.get(c).finish(partition.docCount);
        }
        // Each column owns its recycler-backed buffers (and, for ARRAY, its child's); close them all with the batch.
        return new EscfBatch(schema, partition.docCount, columns, Releasables.wrap(columns));
    }

    @Override
    public int docCount(int partitionKey) {
        Partition partition = partitionKey < partitions.length ? partitions[partitionKey] : null;
        return partition == null ? 0 : partition.docCount;
    }

    @Override
    public boolean hasPartition(int partitionKey) {
        return partitionKey < partitions.length && partitions[partitionKey] != null;
    }

    @Override
    public String columnPath(int columnIndex) {
        if (columnIndex >= cachedPath.length) {
            cachedPath = Arrays.copyOf(cachedPath, Integer.highestOneBit(columnIndex) << 1);
        }
        String path = cachedPath[columnIndex];
        if (path == null) {
            path = schema.getFullPath(columnIndex);
            cachedPath[columnIndex] = path;
        }
        return path;
    }

    @Override
    public void close() {
        for (Partition partition : partitions) {
            if (partition != null) {
                for (EscfColumnBuilder builder : partition.builders) {
                    builder.discard();
                }
            }
        }
        Arrays.fill(partitions, null);
    }

    /** Convenience: encodes all {@code sources} into a single-partition batch. */
    public static EscfBatch encode(List<BytesReference> sources, XContentType xContentType) throws IOException {
        try (EscfEncoder encoder = new EscfEncoder()) {
            for (BytesReference source : sources) {
                encoder.addDocument(source, xContentType, 0);
            }
            return encoder.buildPartition(0);
        }
    }

    private void appendScratchValue(EscfColumnBuilder builder, int columnIndex) {
        final byte type = scratchType[columnIndex];
        switch (type) {
            case SourceValueType.ABSENT -> builder.addAbsent();
            case SourceValueType.NULL -> builder.addNull();
            case SourceValueType.TRUE -> builder.addBoolean(true);
            case SourceValueType.FALSE -> builder.addBoolean(false);
            case SourceValueType.INT, SourceValueType.LONG -> builder.addLong(scratchNumeric[columnIndex]);
            case SourceValueType.FLOAT, SourceValueType.DOUBLE -> builder.addDouble(Double.longBitsToDouble(scratchNumeric[columnIndex]));
            case SourceValueType.STRING -> builder.addString((XContentString.UTF8Bytes) scratchVar[columnIndex]);
            case SourceValueType.FIXED_ARRAY, SourceValueType.UNION_ARRAY -> builder.addArray(type, (byte[]) scratchVar[columnIndex]);
            case SourceValueType.KEY_VALUE -> builder.addKeyValue((byte[]) scratchVar[columnIndex]);
            default -> throw new IllegalStateException("unexpected scratch EIRF type [" + SourceValueType.name(type) + "]");
        }
    }

    private void flattenObject(XContentParser parser, int parentNonLeafIdx, XContentParser.Token firstToken, LeafSink sink)
        throws IOException {
        XContentParser.Token token = firstToken;
        while (token != XContentParser.Token.END_OBJECT) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new IllegalStateException("Expected FIELD_NAME but got " + token);
            }
            String fieldName = parser.currentName();
            token = parser.nextToken();

            if (token == XContentParser.Token.START_OBJECT) {
                // Peek inside the object. An empty object is encoded as its own zero-byte KEY_VALUE leaf so
                // it stays distinguishable from an absent field; non-empty objects flatten recursively.
                XContentParser.Token inner = parser.nextToken();
                if (inner == XContentParser.Token.END_OBJECT) {
                    int colIdx = schema.appendLeaf(fieldName, parentNonLeafIdx);
                    ensureScratchCapacity(colIdx + 1);
                    if (columnsSet.getAndSet(colIdx)) {
                        throw new IllegalArgumentException("Duplicate field [" + fieldName + "]");
                    }
                    scratchType[colIdx] = SourceValueType.KEY_VALUE;
                    scratchVar[colIdx] = BytesRef.EMPTY_BYTES;
                } else {
                    int nonLeafIdx = schema.appendNonLeaf(fieldName, parentNonLeafIdx);
                    flattenObject(parser, nonLeafIdx, inner, sink);
                }
                token = parser.nextToken();
                continue;
            }

            int colIdx = schema.appendLeaf(fieldName, parentNonLeafIdx);
            ensureScratchCapacity(colIdx + 1);
            if (columnsSet.getAndSet(colIdx)) {
                throw new IllegalArgumentException("Duplicate field [" + fieldName + "]");
            }

            final boolean firePathSink = sink != LeafSink.NO_OP;
            final boolean rawTextMode = firePathSink && sink.passRawText();
            switch (token) {
                case START_ARRAY -> {
                    SourceBatchEncodeHelper.PackedArray arr = SourceBatchEncodeHelper.packArray(parser);
                    scratchType[colIdx] = arr.arrayType();
                    scratchVar[colIdx] = arr.packed();
                    if (firePathSink) {
                        sink.onArrayLeaf(colIdx, columnPath(colIdx));
                    }
                }
                case VALUE_STRING -> {
                    XContentString.UTF8Bytes str = parser.optimizedText().bytes();
                    scratchType[colIdx] = SourceValueType.STRING;
                    scratchVar[colIdx] = str;
                    if (firePathSink) {
                        sink.onTextPrimitive(colIdx, columnPath(colIdx), SourceValueType.STRING, str);
                    }
                }
                case VALUE_NUMBER -> {
                    XContentParser.NumberType numType = parser.numberType();
                    switch (numType) {
                        case INT, LONG -> {
                            long val = parser.longValue();
                            byte type = (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE) ? SourceValueType.INT : SourceValueType.LONG;
                            scratchType[colIdx] = type;
                            scratchNumeric[colIdx] = val;
                            if (rawTextMode) {
                                sink.onTextPrimitive(colIdx, columnPath(colIdx), type, parser.optimizedText().bytes());
                            } else if (firePathSink) {
                                sink.onLongPrimitive(colIdx, columnPath(colIdx), type, val);
                            }
                        }
                        case FLOAT, DOUBLE -> {
                            double val = parser.doubleValue();
                            float fval = (float) val;
                            byte type = ((double) fval == val) ? SourceValueType.FLOAT : SourceValueType.DOUBLE;
                            scratchType[colIdx] = type;
                            scratchNumeric[colIdx] = Double.doubleToRawLongBits(val);
                            if (rawTextMode) {
                                sink.onTextPrimitive(colIdx, columnPath(colIdx), type, parser.optimizedText().bytes());
                            } else if (firePathSink) {
                                sink.onDoublePrimitive(colIdx, columnPath(colIdx), type, val);
                            }
                        }
                        default -> {
                            XContentString.UTF8Bytes str = parser.optimizedText().bytes();
                            scratchType[colIdx] = SourceValueType.STRING;
                            scratchVar[colIdx] = str;
                            if (firePathSink) {
                                sink.onTextPrimitive(colIdx, columnPath(colIdx), SourceValueType.STRING, str);
                            }
                        }
                    }
                }
                case VALUE_BOOLEAN -> {
                    boolean v = parser.booleanValue();
                    byte type = v ? SourceValueType.TRUE : SourceValueType.FALSE;
                    scratchType[colIdx] = type;
                    if (rawTextMode) {
                        sink.onTextPrimitive(colIdx, columnPath(colIdx), type, parser.optimizedText().bytes());
                    } else if (firePathSink) {
                        sink.onBooleanPrimitive(colIdx, columnPath(colIdx), v);
                    }
                }
                case VALUE_NULL -> scratchType[colIdx] = SourceValueType.NULL;
                default -> throw new IllegalStateException("Unexpected token: " + token);
            }
            token = parser.nextToken();
        }
    }

    private void ensureScratchCapacity(int size) {
        if (size <= scratchType.length) {
            return;
        }
        int cap = scratchType.length;
        while (cap < size) {
            cap <<= 1;
        }
        scratchType = Arrays.copyOf(scratchType, cap);
        scratchNumeric = Arrays.copyOf(scratchNumeric, cap);
        scratchVar = Arrays.copyOf(scratchVar, cap);
        columnsSet = FixedBitSet.ensureCapacity(columnsSet, cap);
    }

    private Partition getOrCreatePartition(int partitionKey) {
        if (partitionKey >= partitions.length) {
            int newCap = partitions.length;
            while (partitionKey >= newCap) {
                newCap <<= 1;
            }
            partitions = Arrays.copyOf(partitions, newCap);
        }
        Partition partition = partitions[partitionKey];
        if (partition == null) {
            partition = new Partition();
            partitions[partitionKey] = partition;
        }
        return partition;
    }

    private void ensurePartitionBuilders(Partition partition, int size) {
        while (partition.builders.size() < size) {
            EscfColumnBuilder builder = new EscfColumnBuilder(recycler);
            for (int i = 0; i < partition.docCount; i++) {
                builder.addAbsent();
            }
            partition.builders.add(builder);
        }
    }

    private static final class Partition {
        final List<EscfColumnBuilder> builders = new ArrayList<>(INITIAL_CAPACITY);
        int docCount;
    }
}
