/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2026 Elasticsearch B.V.
 */
package org.elasticsearch.index.codec.lucene50.compressing;

import org.apache.lucene.backward_codecs.compressing.CompressionMode;
import org.apache.lucene.backward_codecs.compressing.Decompressor;
import org.apache.lucene.backward_codecs.store.EndiannessReverserUtil;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.StoredFieldDataInput;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.core.IOUtils;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;

/**
 * {@link StoredFieldsReader} impl for {@link Lucene50CompressingStoredFieldsFormat}.
 *
 * @lucene.experimental
 */
public final class Lucene50CompressingStoredFieldsReader extends StoredFieldsReader {

    /** Extension of stored fields file */
    public static final String FIELDS_EXTENSION = "fdt";

    /** Extension of stored fields index */
    public static final String INDEX_EXTENSION = "fdx";

    /** Extension of stored fields meta */
    public static final String META_EXTENSION = "fdm";

    /** Codec name for the index. */
    public static final String INDEX_CODEC_NAME = "Lucene85FieldsIndex";

    static final int STRING = 0x00;
    static final int BYTE_ARR = 0x01;
    static final int NUMERIC_INT = 0x02;
    static final int NUMERIC_FLOAT = 0x03;
    static final int NUMERIC_LONG = 0x04;
    static final int NUMERIC_DOUBLE = 0x05;

    static final int TYPE_BITS = PackedInts.bitsRequired(NUMERIC_DOUBLE);
    static final int TYPE_MASK = (int) PackedInts.maxValue(TYPE_BITS);

    static final int VERSION_START = 1;
    static final int VERSION_OFFHEAP_INDEX = 2;

    /** Version where all metadata were moved to the meta file. */
    static final int VERSION_META = 3;

    /**
     * Version where numChunks is explicitly recorded in meta file and a dirty chunk bit is recorded
     * in each chunk
     */
    static final int VERSION_NUM_CHUNKS = 4;

    static final int VERSION_CURRENT = VERSION_NUM_CHUNKS;
    static final int META_VERSION_START = 0;

    // for compression of timestamps
    static final long SECOND = 1000L;
    static final long HOUR = 60 * 60 * SECOND;
    static final long DAY = 24 * HOUR;
    static final int SECOND_ENCODING = 0x40;
    static final int HOUR_ENCODING = 0x80;
    static final int DAY_ENCODING = 0xC0;

    private final int version;
    private final FieldInfos fieldInfos;
    private final FieldsIndex indexReader;
    private final long maxPointer;
    private final IndexInput fieldsStream;
    private final int chunkSize;
    private final int packedIntsVersion;
    private final CompressionMode compressionMode;
    private final Decompressor decompressor;
    private final int numDocs;
    private final boolean merging;
    private final BlockState state;
    private boolean closed;

    // used by clone
    private Lucene50CompressingStoredFieldsReader(Lucene50CompressingStoredFieldsReader reader, boolean merging) {
        this.version = reader.version;
        this.fieldInfos = reader.fieldInfos;
        this.fieldsStream = reader.fieldsStream.clone();
        this.indexReader = reader.indexReader.clone();
        this.maxPointer = reader.maxPointer;
        this.chunkSize = reader.chunkSize;
        this.packedIntsVersion = reader.packedIntsVersion;
        this.compressionMode = reader.compressionMode;
        this.decompressor = reader.decompressor.clone();
        this.numDocs = reader.numDocs;
        this.merging = merging;
        this.state = new BlockState();
        this.closed = false;
    }

    /** Sole constructor. */
    public Lucene50CompressingStoredFieldsReader(
        Directory d,
        SegmentInfo si,
        String segmentSuffix,
        FieldInfos fn,
        IOContext context,
        String formatName,
        CompressionMode compressionMode
    ) throws IOException {
        this.compressionMode = compressionMode;
        final String segment = si.name;
        boolean success = false;
        fieldInfos = fn;
        numDocs = si.maxDoc();

        final String fieldsStreamFN = IndexFileNames.segmentFileName(segment, segmentSuffix, FIELDS_EXTENSION);
        ChecksumIndexInput metaIn = null;
        try {
            // Open the data file
            fieldsStream = EndiannessReverserUtil.openInput(d, fieldsStreamFN, context);
            version = CodecUtil.checkIndexHeader(fieldsStream, formatName, VERSION_START, VERSION_CURRENT, si.getId(), segmentSuffix);
            assert CodecUtil.indexHeaderLength(formatName, segmentSuffix) == fieldsStream.getFilePointer();

            if (version >= VERSION_OFFHEAP_INDEX) {
                final String metaStreamFN = IndexFileNames.segmentFileName(segment, segmentSuffix, META_EXTENSION);
                metaIn = EndiannessReverserUtil.openChecksumInput(d, metaStreamFN, IOContext.READONCE);
                CodecUtil.checkIndexHeader(metaIn, INDEX_CODEC_NAME + "Meta", META_VERSION_START, version, si.getId(), segmentSuffix);
            }
            if (version >= VERSION_META) {
                chunkSize = metaIn.readVInt();
                packedIntsVersion = metaIn.readVInt();
            } else {
                chunkSize = fieldsStream.readVInt();
                packedIntsVersion = fieldsStream.readVInt();
            }

            decompressor = compressionMode.newDecompressor();
            this.merging = false;
            this.state = new BlockState();

            // NOTE: data file is too costly to verify checksum against all the bytes on open,
            // but for now we at least verify proper structure of the checksum footer: which looks
            // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
            // such as file truncation.
            CodecUtil.retrieveChecksum(fieldsStream);

            long maxPointer = -1;
            FieldsIndex indexReader = null;

            if (version < VERSION_OFFHEAP_INDEX) {
                // Load the index into memory
                final String indexName = IndexFileNames.segmentFileName(segment, segmentSuffix, "fdx");
                try (ChecksumIndexInput indexStream = EndiannessReverserUtil.openChecksumInput(d, indexName, context)) {
                    Throwable priorE = null;
                    try {
                        assert formatName.endsWith("Data");
                        final String codecNameIdx = formatName.substring(0, formatName.length() - "Data".length()) + "Index";
                        final int version2 = CodecUtil.checkIndexHeader(
                            indexStream,
                            codecNameIdx,
                            VERSION_START,
                            VERSION_CURRENT,
                            si.getId(),
                            segmentSuffix
                        );
                        if (version != version2) {
                            throw new CorruptIndexException(
                                "Version mismatch between stored fields index and data: " + version2 + " != " + version,
                                indexStream
                            );
                        }
                        assert CodecUtil.indexHeaderLength(codecNameIdx, segmentSuffix) == indexStream.getFilePointer();
                        indexReader = new LegacyFieldsIndexReader(indexStream, si);
                        maxPointer = indexStream.readVLong();
                    } catch (Throwable exception) {
                        priorE = exception;
                    } finally {
                        CodecUtil.checkFooter(indexStream, priorE);
                    }
                }
            } else {
                FieldsIndexReader fieldsIndexReader = new FieldsIndexReader(
                    d,
                    si.name,
                    segmentSuffix,
                    INDEX_EXTENSION,
                    INDEX_CODEC_NAME,
                    si.getId(),
                    metaIn
                );
                indexReader = fieldsIndexReader;
                maxPointer = fieldsIndexReader.getMaxPointer();
            }

            this.maxPointer = maxPointer;
            this.indexReader = indexReader;

            if (version >= VERSION_NUM_CHUNKS) {
                // discard num_chunks
                metaIn.readVLong();
            }
            if (version >= VERSION_META) {
                // consume dirty chunks/docs stats we wrote
                metaIn.readVLong();
                metaIn.readVLong();
            }

            if (metaIn != null) {
                CodecUtil.checkFooter(metaIn, null);
                metaIn.close();
            }

            success = true;
        } catch (Throwable t) {
            if (metaIn != null) {
                CodecUtil.checkFooter(metaIn, t);
                throw new AssertionError("unreachable");
            } else {
                throw t;
            }
        } finally {
            if (!success) {
                IOUtils.closeWhileHandlingException(this, metaIn);
            }
        }
    }

    /**
     * @throws AlreadyClosedException if this FieldsReader is closed
     */
    private void ensureOpen() throws AlreadyClosedException {
        if (closed) {
            throw new AlreadyClosedException("this FieldsReader is closed");
        }
    }

    /** Close the underlying {@link IndexInput}s. */
    @Override
    public void close() throws IOException {
        if (!closed) {
            IOUtils.close(indexReader, fieldsStream);
            closed = true;
        }
    }

    private static void readField(DataInput in, StoredFieldVisitor visitor, FieldInfo info, int bits) throws IOException {
        switch (bits & TYPE_MASK) {
            case BYTE_ARR:
                int length = in.readVInt();
                visitor.binaryField(info, new StoredFieldDataInput(in, length));
                break;
            case STRING:
                visitor.stringField(info, in.readString());
                break;
            case NUMERIC_INT:
                visitor.intField(info, in.readZInt());
                break;
            case NUMERIC_FLOAT:
                visitor.floatField(info, readZFloat(in));
                break;
            case NUMERIC_LONG:
                visitor.longField(info, readTLong(in));
                break;
            case NUMERIC_DOUBLE:
                visitor.doubleField(info, readZDouble(in));
                break;
            default:
                throw new AssertionError("Unknown type flag: " + Integer.toHexString(bits));
        }
    }

    private static void skipField(DataInput in, int bits) throws IOException {
        switch (bits & TYPE_MASK) {
            case BYTE_ARR:
            case STRING:
                final int length = in.readVInt();
                in.skipBytes(length);
                break;
            case NUMERIC_INT:
                in.readZInt();
                break;
            case NUMERIC_FLOAT:
                readZFloat(in);
                break;
            case NUMERIC_LONG:
                readTLong(in);
                break;
            case NUMERIC_DOUBLE:
                readZDouble(in);
                break;
            default:
                throw new AssertionError("Unknown type flag: " + Integer.toHexString(bits));
        }
    }

    /**
     * Reads a float in a variable-length format. Reads between one and five bytes. Small integral
     * values typically take fewer bytes.
     */
    static float readZFloat(DataInput in) throws IOException {
        int b = in.readByte() & 0xFF;
        if (b == 0xFF) {
            // negative value
            return Float.intBitsToFloat(in.readInt());
        } else if ((b & 0x80) != 0) {
            // small integer [-1..125]
            return (b & 0x7f) - 1;
        } else {
            // positive float
            int bits = b << 24 | ((in.readShort() & 0xFFFF) << 8) | (in.readByte() & 0xFF);
            return Float.intBitsToFloat(bits);
        }
    }

    /**
     * Reads a double in a variable-length format. Reads between one and nine bytes. Small integral
     * values typically take fewer bytes.
     */
    static double readZDouble(DataInput in) throws IOException {
        int b = in.readByte() & 0xFF;
        if (b == 0xFF) {
            // negative value
            return Double.longBitsToDouble(in.readLong());
        } else if (b == 0xFE) {
            // float
            return Float.intBitsToFloat(in.readInt());
        } else if ((b & 0x80) != 0) {
            // small integer [-1..124]
            return (b & 0x7f) - 1;
        } else {
            // positive double
            long bits = ((long) b) << 56 | ((in.readInt() & 0xFFFFFFFFL) << 24) | ((in.readShort() & 0xFFFFL) << 8) | (in.readByte()
                & 0xFFL);
            return Double.longBitsToDouble(bits);
        }
    }

    /**
     * Reads a long in a variable-length format. Reads between one andCorePropLo nine bytes. Small
     * values typically take fewer bytes.
     */
    static long readTLong(DataInput in) throws IOException {
        int header = in.readByte() & 0xFF;

        long bits = header & 0x1F;
        if ((header & 0x20) != 0) {
            // continuation bit
            bits |= in.readVLong() << 5;
        }

        long l = BitUtil.zigZagDecode(bits);

        switch (header & DAY_ENCODING) {
            case SECOND_ENCODING:
                l *= SECOND;
                break;
            case HOUR_ENCODING:
                l *= HOUR;
                break;
            case DAY_ENCODING:
                l *= DAY;
                break;
            case 0:
                // uncompressed
                break;
            default:
                throw new AssertionError();
        }

        return l;
    }

    /**
     * A serialized document, you need to decode its input in order to get an actual {@link Document}.
     */
    static class SerializedDocument {

        // the serialized data
        final DataInput in;

        // the number of bytes on which the document is encoded
        final int length;

        // the number of stored fields
        final int numStoredFields;

        private SerializedDocument(DataInput in, int length, int numStoredFields) {
            this.in = in;
            this.length = length;
            this.numStoredFields = numStoredFields;
        }
    }

    /** Keeps state about the current block of documents. */
    private class BlockState {

        private int docBase, chunkDocs;

        // whether the block has been sliced, this happens for large documents
        private boolean sliced;

        private long[] offsets = LongsRef.EMPTY_LONGS;
        private long[] numStoredFields = LongsRef.EMPTY_LONGS;

        // the start pointer at which you can read the compressed documents
        private long startPointer;

        private final BytesRef spare;
        private final BytesRef bytes;

        BlockState() {
            if (merging) {
                spare = new BytesRef();
                bytes = new BytesRef();
            } else {
                spare = bytes = null;
            }
        }

        boolean contains(int docID) {
            return docID >= docBase && docID < docBase + chunkDocs;
        }

        /** Reset this block so that it stores state for the block that contains the given doc id. */
        void reset(int docID) throws IOException {
            boolean success = false;
            try {
                doReset(docID);
                success = true;
            } finally {
                if (success == false) {
                    // if the read failed, set chunkDocs to 0 so that it does not
                    // contain any docs anymore and is not reused. This should help
                    // get consistent exceptions when trying to get several
                    // documents which are in the same corrupted block since it will
                    // force the header to be decoded again
                    chunkDocs = 0;
                }
            }
        }

        private void doReset(int docID) throws IOException {
            docBase = fieldsStream.readVInt();
            final int token = fieldsStream.readVInt();
            chunkDocs = version >= VERSION_NUM_CHUNKS ? token >>> 2 : token >>> 1;
            if (contains(docID) == false || docBase + chunkDocs > numDocs) {
                throw new CorruptIndexException(
                    "Corrupted: docID=" + docID + ", docBase=" + docBase + ", chunkDocs=" + chunkDocs + ", numDocs=" + numDocs,
                    fieldsStream
                );
            }

            sliced = (token & 1) != 0;

            offsets = ArrayUtil.grow(offsets, chunkDocs + 1);
            numStoredFields = ArrayUtil.grow(numStoredFields, chunkDocs);

            if (chunkDocs == 1) {
                numStoredFields[0] = fieldsStream.readVInt();
                offsets[1] = fieldsStream.readVInt();
            } else {
                // Number of stored fields per document
                final int bitsPerStoredFields = fieldsStream.readVInt();
                if (bitsPerStoredFields == 0) {
                    Arrays.fill(numStoredFields, 0, chunkDocs, fieldsStream.readVInt());
                } else if (bitsPerStoredFields > 31) {
                    throw new CorruptIndexException("bitsPerStoredFields=" + bitsPerStoredFields, fieldsStream);
                } else {
                    final PackedInts.ReaderIterator it = PackedInts.getReaderIteratorNoHeader(
                        fieldsStream,
                        PackedInts.Format.PACKED,
                        packedIntsVersion,
                        chunkDocs,
                        bitsPerStoredFields,
                        1024
                    );
                    for (int i = 0; i < chunkDocs;) {
                        final LongsRef next = it.next(Integer.MAX_VALUE);
                        System.arraycopy(next.longs, next.offset, numStoredFields, i, next.length);
                        i += next.length;
                    }
                }

                // The stream encodes the length of each document and we decode
                // it into a list of monotonically increasing offsets
                final int bitsPerLength = fieldsStream.readVInt();
                if (bitsPerLength == 0) {
                    final int length = fieldsStream.readVInt();
                    for (int i = 0; i < chunkDocs; ++i) {
                        offsets[1 + i] = (1 + i) * (long) length;
                    }
                } else if (bitsPerStoredFields > 31) {
                    throw new CorruptIndexException("bitsPerLength=" + bitsPerLength, fieldsStream);
                } else {
                    final PackedInts.ReaderIterator it = PackedInts.getReaderIteratorNoHeader(
                        fieldsStream,
                        PackedInts.Format.PACKED,
                        packedIntsVersion,
                        chunkDocs,
                        bitsPerLength,
                        1024
                    );
                    for (int i = 0; i < chunkDocs;) {
                        final LongsRef next = it.next(Integer.MAX_VALUE);
                        System.arraycopy(next.longs, next.offset, offsets, i + 1, next.length);
                        i += next.length;
                    }
                    for (int i = 0; i < chunkDocs; ++i) {
                        offsets[i + 1] += offsets[i];
                    }
                }

                // Additional validation: only the empty document has a serialized length of 0
                for (int i = 0; i < chunkDocs; ++i) {
                    final long len = offsets[i + 1] - offsets[i];
                    final long storedFields = numStoredFields[i];
                    if ((len == 0) != (storedFields == 0)) {
                        throw new CorruptIndexException("length=" + len + ", numStoredFields=" + storedFields, fieldsStream);
                    }
                }
            }

            startPointer = fieldsStream.getFilePointer();

            if (merging) {
                final int totalLength = Math.toIntExact(offsets[chunkDocs]);
                // decompress eagerly
                if (sliced) {
                    bytes.offset = bytes.length = 0;
                    for (int decompressed = 0; decompressed < totalLength;) {
                        final int toDecompress = Math.min(totalLength - decompressed, chunkSize);
                        decompressor.decompress(fieldsStream, toDecompress, 0, toDecompress, spare);
                        bytes.bytes = ArrayUtil.grow(bytes.bytes, bytes.length + spare.length);
                        System.arraycopy(spare.bytes, spare.offset, bytes.bytes, bytes.length, spare.length);
                        bytes.length += spare.length;
                        decompressed += toDecompress;
                    }
                } else {
                    decompressor.decompress(fieldsStream, totalLength, 0, totalLength, bytes);
                }
                if (bytes.length != totalLength) {
                    throw new CorruptIndexException(
                        "Corrupted: expected chunk size = " + totalLength + ", got " + bytes.length,
                        fieldsStream
                    );
                }
            }
        }

        /**
         * Get the serialized representation of the given docID. This docID has to be contained in the
         * current block.
         */
        SerializedDocument document(int docID) throws IOException {
            if (contains(docID) == false) {
                throw new IllegalArgumentException();
            }

            final int index = docID - docBase;
            final int offset = Math.toIntExact(offsets[index]);
            final int length = Math.toIntExact(offsets[index + 1]) - offset;
            final int totalLength = Math.toIntExact(offsets[chunkDocs]);
            final int numStoredFields = Math.toIntExact(this.numStoredFields[index]);

            final BytesRef bytes;
            if (merging) {
                bytes = this.bytes;
            } else {
                bytes = new BytesRef();
            }

            final DataInput documentInput;
            if (length == 0) {
                // empty
                documentInput = new ByteArrayDataInput();
            } else if (merging) {
                // already decompressed
                documentInput = new ByteArrayDataInput(bytes.bytes, bytes.offset + offset, length);
            } else if (sliced) {
                fieldsStream.seek(startPointer);
                decompressor.decompress(fieldsStream, chunkSize, offset, Math.min(length, chunkSize - offset), bytes);
                documentInput = new DataInput() {

                    int decompressed = bytes.length;

                    void fillBuffer() throws IOException {
                        assert decompressed <= length;
                        if (decompressed == length) {
                            throw new EOFException();
                        }
                        final int toDecompress = Math.min(length - decompressed, chunkSize);
                        decompressor.decompress(fieldsStream, toDecompress, 0, toDecompress, bytes);
                        decompressed += toDecompress;
                    }

                    @Override
                    public byte readByte() throws IOException {
                        if (bytes.length == 0) {
                            fillBuffer();
                        }
                        --bytes.length;
                        return bytes.bytes[bytes.offset++];
                    }

                    @Override
                    public void readBytes(byte[] b, int offset, int len) throws IOException {
                        while (len > bytes.length) {
                            System.arraycopy(bytes.bytes, bytes.offset, b, offset, bytes.length);
                            len -= bytes.length;
                            offset += bytes.length;
                            fillBuffer();
                        }
                        System.arraycopy(bytes.bytes, bytes.offset, b, offset, len);
                        bytes.offset += len;
                        bytes.length -= len;
                    }

                    @Override
                    public void skipBytes(long numBytes) throws IOException {
                        if (numBytes < 0) {
                            throw new IllegalArgumentException("numBytes must be >= 0, got " + numBytes);
                        }
                        while (numBytes > bytes.length) {
                            numBytes -= bytes.length;
                            fillBuffer();
                        }
                        bytes.offset += Math.toIntExact(numBytes);
                        bytes.length -= Math.toIntExact(numBytes);
                    }
                };
            } else {
                fieldsStream.seek(startPointer);
                decompressor.decompress(fieldsStream, totalLength, offset, length, bytes);
                assert bytes.length == length;
                documentInput = new ByteArrayDataInput(bytes.bytes, bytes.offset, bytes.length);
            }

            return new SerializedDocument(EndiannessReverserUtil.wrapDataInput(documentInput), length, numStoredFields);
        }
    }

    SerializedDocument serializedDocument(int docID) throws IOException {
        if (state.contains(docID) == false) {
            fieldsStream.seek(indexReader.getStartPointer(docID));
            state.reset(docID);
        }
        assert state.contains(docID);
        return state.document(docID);
    }

    @Override
    public void document(int docID, StoredFieldVisitor visitor) throws IOException {

        final SerializedDocument doc = serializedDocument(docID);

        for (int fieldIDX = 0; fieldIDX < doc.numStoredFields; fieldIDX++) {
            final long infoAndBits = doc.in.readVLong();
            final int fieldNumber = (int) (infoAndBits >>> TYPE_BITS);
            final FieldInfo fieldInfo = fieldInfos.fieldInfo(fieldNumber);

            final int bits = (int) (infoAndBits & TYPE_MASK);
            assert bits <= NUMERIC_DOUBLE : "bits=" + Integer.toHexString(bits);

            switch (visitor.needsField(fieldInfo)) {
                case YES:
                    readField(doc.in, visitor, fieldInfo, bits);
                    break;
                case NO:
                    if (fieldIDX == doc.numStoredFields - 1) { // don't skipField on last field value; treat like STOP
                        return;
                    }
                    skipField(doc.in, bits);
                    break;
                case STOP:
                    return;
            }
        }
    }

    @Override
    public StoredFieldsReader clone() {
        ensureOpen();
        return new Lucene50CompressingStoredFieldsReader(this, false);
    }

    @Override
    public StoredFieldsReader getMergeInstance() {
        ensureOpen();
        return new Lucene50CompressingStoredFieldsReader(this, true);
    }

    @Override
    public void checkIntegrity(MergePolicy.OneMerge merge) throws IOException {
        indexReader.checkIntegrity();
        CodecUtil.checksumEntireFile(fieldsStream, merge);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(mode=" + compressionMode + ",chunksize=" + chunkSize + ")";
    }
}
