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
package org.elasticsearch.index.codec.lucene80;

import org.apache.lucene.backward_codecs.store.EndiannessReverserUtil;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.codec.lucene80.Lucene80NormsFormat.VERSION_CURRENT;
import static org.elasticsearch.index.codec.lucene80.Lucene80NormsFormat.VERSION_START;

/** Reader for {@link Lucene80NormsFormat} */
final class Lucene80NormsProducer extends NormsProducer implements Cloneable {
    // metadata maps (just file pointers and minimal stuff)
    private final Map<Integer, NormsEntry> norms = new HashMap<>();
    private final int maxDoc;
    private IndexInput data;
    private boolean merging;
    private Map<Integer, IndexInput> disiInputs;
    private Map<Integer, RandomAccessInput> disiJumpTables;
    private Map<Integer, RandomAccessInput> dataInputs;

    Lucene80NormsProducer(SegmentReadState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension)
        throws IOException {
        maxDoc = state.segmentInfo.maxDoc();
        String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
        int version = -1;

        // read in the entries from the metadata file.
        try (ChecksumIndexInput in = EndiannessReverserUtil.openChecksumInput(state.directory, metaName, state.context)) {
            Throwable priorE = null;
            try {
                version = CodecUtil.checkIndexHeader(
                    in,
                    metaCodec,
                    VERSION_START,
                    VERSION_CURRENT,
                    state.segmentInfo.getId(),
                    state.segmentSuffix
                );
                readFields(in, state.fieldInfos);
            } catch (Throwable exception) {
                priorE = exception;
            } finally {
                CodecUtil.checkFooter(in, priorE);
            }
        }

        String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
        data = EndiannessReverserUtil.openInput(state.directory, dataName, state.context);
        boolean success = false;
        try {
            final int version2 = CodecUtil.checkIndexHeader(
                data,
                dataCodec,
                VERSION_START,
                VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            if (version != version2) {
                throw new CorruptIndexException("Format versions mismatch: meta=" + version + ",data=" + version2, data);
            }

            // NOTE: data file is too costly to verify checksum against all the bytes on open,
            // but for now we at least verify proper structure of the checksum footer: which looks
            // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
            // such as file truncation.
            CodecUtil.retrieveChecksum(data);

            success = true;
        } finally {
            if (!success) {
                IOUtils.closeWhileHandlingException(this.data);
            }
        }
    }

    @Override
    public NormsProducer getMergeInstance() {
        Lucene80NormsProducer clone;
        try {
            clone = (Lucene80NormsProducer) super.clone();
        } catch (CloneNotSupportedException e) {
            // cannot happen
            throw new RuntimeException(e);
        }
        clone.data = data.clone();
        clone.disiInputs = new HashMap<>();
        clone.disiJumpTables = new HashMap<>();
        clone.dataInputs = new HashMap<>();
        clone.merging = true;
        return clone;
    }

    static class NormsEntry {
        byte denseRankPower;
        byte bytesPerNorm;
        long docsWithFieldOffset;
        long docsWithFieldLength;
        short jumpTableEntryCount;
        int numDocsWithField;
        long normsOffset;
    }

    abstract static class DenseNormsIterator extends NumericDocValues {

        final int maxDoc;
        int doc = -1;

        DenseNormsIterator(int maxDoc) {
            this.maxDoc = maxDoc;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() throws IOException {
            return advance(doc + 1);
        }

        @Override
        public int advance(int target) throws IOException {
            if (target >= maxDoc) {
                return doc = NO_MORE_DOCS;
            }
            return doc = target;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            this.doc = target;
            return true;
        }

        @Override
        public long cost() {
            return maxDoc;
        }
    }

    abstract static class SparseNormsIterator extends NumericDocValues {

        final IndexedDISI disi;

        SparseNormsIterator(IndexedDISI disi) {
            this.disi = disi;
        }

        @Override
        public int docID() {
            return disi.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            return disi.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            return disi.advance(target);
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return disi.advanceExact(target);
        }

        @Override
        public long cost() {
            return disi.cost();
        }
    }

    private void readFields(IndexInput meta, FieldInfos infos) throws IOException {
        for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
            FieldInfo info = infos.fieldInfo(fieldNumber);
            if (info == null) {
                throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
            } else if (!info.hasNorms()) {
                throw new CorruptIndexException("Invalid field: " + info.name, meta);
            }
            NormsEntry entry = new NormsEntry();
            entry.docsWithFieldOffset = meta.readLong();
            entry.docsWithFieldLength = meta.readLong();
            entry.jumpTableEntryCount = meta.readShort();
            entry.denseRankPower = meta.readByte();
            entry.numDocsWithField = meta.readInt();
            entry.bytesPerNorm = meta.readByte();
            switch (entry.bytesPerNorm) {
                case 0:
                case 1:
                case 2:
                case 4:
                case 8:
                    break;
                default:
                    throw new CorruptIndexException("Invalid bytesPerValue: " + entry.bytesPerNorm + ", field: " + info.name, meta);
            }
            entry.normsOffset = meta.readLong();
            norms.put(info.number, entry);
        }
    }

    private RandomAccessInput getDataInput(FieldInfo field, NormsEntry entry) throws IOException {
        RandomAccessInput slice = null;
        if (merging) {
            slice = dataInputs.get(field.number);
        }
        if (slice == null) {
            slice = data.randomAccessSlice(entry.normsOffset, entry.numDocsWithField * (long) entry.bytesPerNorm);
            if (merging) {
                dataInputs.put(field.number, slice);
            }
        }
        return slice;
    }

    private IndexInput getDisiInput(FieldInfo field, NormsEntry entry) throws IOException {
        if (merging == false) {
            return IndexedDISI.createBlockSlice(
                data,
                "docs",
                entry.docsWithFieldOffset,
                entry.docsWithFieldLength,
                entry.jumpTableEntryCount
            );
        }

        IndexInput in = disiInputs.get(field.number);
        if (in == null) {
            in = IndexedDISI.createBlockSlice(
                data,
                "docs",
                entry.docsWithFieldOffset,
                entry.docsWithFieldLength,
                entry.jumpTableEntryCount
            );
            disiInputs.put(field.number, in);
        }

        final IndexInput inF = in; // same as in but final

        // Wrap so that reads can be interleaved from the same thread if two
        // norms instances are pulled and consumed in parallel. Merging usually
        // doesn't need this feature but CheckIndex might, plus we need merge
        // instances to behave well and not be trappy.
        return new IndexInput("docs") {

            long offset = 0;

            @Override
            public void readBytes(byte[] b, int off, int len) throws IOException {
                inF.seek(offset);
                offset += len;
                inF.readBytes(b, off, len);
            }

            @Override
            public byte readByte() throws IOException {
                throw new UnsupportedOperationException("Unused by IndexedDISI");
            }

            @Override
            public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
                throw new UnsupportedOperationException("Unused by IndexedDISI");
            }

            @Override
            public short readShort() throws IOException {
                inF.seek(offset);
                offset += Short.BYTES;
                return inF.readShort();
            }

            @Override
            public long readLong() throws IOException {
                inF.seek(offset);
                offset += Long.BYTES;
                return inF.readLong();
            }

            @Override
            public void seek(long pos) throws IOException {
                offset = pos;
            }

            @Override
            public long length() {
                throw new UnsupportedOperationException("Unused by IndexedDISI");
            }

            @Override
            public long getFilePointer() {
                return offset;
            }

            @Override
            public void close() throws IOException {
                throw new UnsupportedOperationException("Unused by IndexedDISI");
            }
        };
    }

    private RandomAccessInput getDisiJumpTable(FieldInfo field, NormsEntry entry) throws IOException {
        RandomAccessInput jumpTable = null;
        if (merging) {
            jumpTable = disiJumpTables.get(field.number);
        }
        if (jumpTable == null) {
            jumpTable = IndexedDISI.createJumpTable(data, entry.docsWithFieldOffset, entry.docsWithFieldLength, entry.jumpTableEntryCount);
            if (merging) {
                disiJumpTables.put(field.number, jumpTable);
            }
        }
        return jumpTable;
    }

    @Override
    public NumericDocValues getNorms(FieldInfo field) throws IOException {
        final NormsEntry entry = norms.get(field.number);
        if (entry.docsWithFieldOffset == -2) {
            // empty
            return DocValues.emptyNumeric();
        } else if (entry.docsWithFieldOffset == -1) {
            // dense
            if (entry.bytesPerNorm == 0) {
                return new DenseNormsIterator(maxDoc) {
                    @Override
                    public long longValue() throws IOException {
                        return entry.normsOffset;
                    }
                };
            }
            final RandomAccessInput slice = getDataInput(field, entry);
            switch (entry.bytesPerNorm) {
                case 1:
                    return new DenseNormsIterator(maxDoc) {
                        @Override
                        public long longValue() throws IOException {
                            return slice.readByte(doc);
                        }
                    };
                case 2:
                    return new DenseNormsIterator(maxDoc) {
                        @Override
                        public long longValue() throws IOException {
                            return slice.readShort(((long) doc) << 1);
                        }
                    };
                case 4:
                    return new DenseNormsIterator(maxDoc) {
                        @Override
                        public long longValue() throws IOException {
                            return slice.readInt(((long) doc) << 2);
                        }
                    };
                case 8:
                    return new DenseNormsIterator(maxDoc) {
                        @Override
                        public long longValue() throws IOException {
                            return slice.readLong(((long) doc) << 3);
                        }
                    };
                default:
                    // should not happen, we already validate bytesPerNorm in readFields
                    throw new AssertionError();
            }
        } else {
            // sparse
            final IndexInput disiInput = getDisiInput(field, entry);
            final RandomAccessInput disiJumpTable = getDisiJumpTable(field, entry);
            final IndexedDISI disi = new IndexedDISI(
                disiInput,
                disiJumpTable,
                entry.jumpTableEntryCount,
                entry.denseRankPower,
                entry.numDocsWithField
            );

            if (entry.bytesPerNorm == 0) {
                return new SparseNormsIterator(disi) {
                    @Override
                    public long longValue() throws IOException {
                        return entry.normsOffset;
                    }
                };
            }
            final RandomAccessInput slice = getDataInput(field, entry);
            switch (entry.bytesPerNorm) {
                case 1:
                    return new SparseNormsIterator(disi) {
                        @Override
                        public long longValue() throws IOException {
                            return slice.readByte(disi.index());
                        }
                    };
                case 2:
                    return new SparseNormsIterator(disi) {
                        @Override
                        public long longValue() throws IOException {
                            return slice.readShort(((long) disi.index()) << 1);
                        }
                    };
                case 4:
                    return new SparseNormsIterator(disi) {
                        @Override
                        public long longValue() throws IOException {
                            return slice.readInt(((long) disi.index()) << 2);
                        }
                    };
                case 8:
                    return new SparseNormsIterator(disi) {
                        @Override
                        public long longValue() throws IOException {
                            return slice.readLong(((long) disi.index()) << 3);
                        }
                    };
                default:
                    // should not happen, we already validate bytesPerNorm in readFields
                    throw new AssertionError();
            }
        }
    }

    @Override
    public void close() throws IOException {
        data.close();
    }

    @Override
    public void checkIntegrity(MergePolicy.OneMerge merge) throws IOException {
        CodecUtil.checksumEntireFile(data, merge);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(fields=" + norms.size() + ")";
    }
}
