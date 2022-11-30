/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.DirectMonotonicReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ES97TSDBDocValuesProducer extends DocValuesProducer {
    private final Map<String, NumericEntry> numerics = new HashMap<>();
    private final IndexInput data;
    private final int maxDoc;

    ES97TSDBDocValuesProducer(SegmentReadState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension)
        throws IOException {
        String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
        this.maxDoc = state.segmentInfo.maxDoc();

        // read in the entries from the metadata file.
        int version = -1;
        try (ChecksumIndexInput in = state.directory.openChecksumInput(metaName, state.context)) {
            Throwable priorE = null;

            try {
                version = CodecUtil.checkIndexHeader(
                    in,
                    metaCodec,
                    ES97TSDBDocValuesFormat.VERSION_START,
                    ES97TSDBDocValuesFormat.VERSION_CURRENT,
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
        this.data = state.directory.openInput(dataName, state.context);
        boolean success = false;
        try {
            final int version2 = CodecUtil.checkIndexHeader(
                data,
                dataCodec,
                ES97TSDBDocValuesFormat.VERSION_START,
                ES97TSDBDocValuesFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            if (version != version2) {
                throw new CorruptIndexException("Format versions mismatch: meta=" + version + ", data=" + version2, data);
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
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        NumericEntry entry = numerics.get(field.name);
        return getNumeric(entry);
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        throw new UnsupportedOperationException("Unsupported binary doc values for field [" + field.name + "]");
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
        throw new UnsupportedOperationException("Unsupported sorted doc values for field [" + field.name + "]");
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        throw new UnsupportedOperationException("Unsupported sorted numeric doc values for field [" + field.name + "]");
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        throw new UnsupportedOperationException("Unsupported sorted set doc values for field [" + field.name + "]");
    }

    @Override
    public void checkIntegrity() throws IOException {
        CodecUtil.checksumEntireFile(data);
    }

    @Override
    public void close() throws IOException {
        data.close();
    }

    private void readFields(IndexInput meta, FieldInfos infos) throws IOException {
        for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
            FieldInfo info = infos.fieldInfo(fieldNumber);
            if (info == null) {
                throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
            }
            numerics.put(info.name, readNumeric(meta));
        }
    }

    private NumericEntry readNumeric(IndexInput meta) throws IOException {
        NumericEntry entry = new NumericEntry();
        readNumeric(meta, entry);
        return entry;
    }

    private void readNumeric(IndexInput meta, NumericEntry entry) throws IOException {
        entry.docsWithFieldOffset = meta.readLong();
        entry.docsWithFieldLength = meta.readLong();
        entry.jumpTableEntryCount = meta.readShort();
        entry.denseRankPower = meta.readByte();
        entry.numValues = meta.readLong();
        if (entry.numValues > 0) {
            final int indexBlockShift = meta.readInt();
            entry.indexMeta = DirectMonotonicReader.loadMeta(
                meta,
                1 + ((entry.numValues - 1) >>> ES97TSDBDocValuesFormat.NUMERIC_BLOCK_SHIFT),
                indexBlockShift
            );
            entry.indexOffset = meta.readLong();
            entry.indexLength = meta.readLong();
            entry.valuesOffset = meta.readLong();
            entry.valuesLength = meta.readLong();
        }
    }

    private static class NumericEntry {
        long docsWithFieldOffset;
        long docsWithFieldLength;
        short jumpTableEntryCount;
        byte denseRankPower;
        long numValues;
        long indexOffset;
        long indexLength;
        DirectMonotonicReader.Meta indexMeta;
        long valuesOffset;
        long valuesLength;
    }

    private NumericDocValues getNumeric(NumericEntry entry) throws IOException {
        if (entry.docsWithFieldOffset == -2) {
            // empty
            return DocValues.emptyNumeric();
        }

        // NOTE: we could make this a bit simpler by reusing #getValues but this
        // makes things slower.

        final RandomAccessInput indexSlice = data.randomAccessSlice(entry.indexOffset, entry.indexLength);
        final DirectMonotonicReader indexReader = DirectMonotonicReader.getInstance(entry.indexMeta, indexSlice);
        final IndexInput valuesData = data.slice("values", entry.valuesOffset, entry.valuesLength);

        if (entry.docsWithFieldOffset == -1) {
            // dense
            return new NumericDocValues() {

                private final int maxDoc = ES97TSDBDocValuesProducer.this.maxDoc;
                private int doc = -1;
                private final ES97TSDBDocValuesEncoder decoder = new ES97TSDBDocValuesEncoder();
                private long currentBlockIndex = -1;
                private final long[] currentBlock = new long[ES97TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE];

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
                public boolean advanceExact(int target) {
                    doc = target;
                    return true;
                }

                @Override
                public long cost() {
                    return maxDoc;
                }

                @Override
                public long longValue() throws IOException {
                    final int index = doc;
                    final int blockIndex = index >>> ES97TSDBDocValuesFormat.NUMERIC_BLOCK_SHIFT;
                    final int blockInIndex = index & ES97TSDBDocValuesFormat.NUMERIC_BLOCK_MASK;
                    if (blockIndex != currentBlockIndex) {
                        assert blockIndex > currentBlockIndex;
                        if (blockIndex - 1 > currentBlockIndex) {
                            valuesData.seek(indexReader.get(blockIndex));
                        }
                        currentBlockIndex = blockIndex;
                        decoder.decode(valuesData, currentBlock);
                    }
                    return currentBlock[blockInIndex];
                }
            };
        } else {
            final IndexedDISI disi = new IndexedDISI(
                data,
                entry.docsWithFieldOffset,
                entry.docsWithFieldLength,
                entry.jumpTableEntryCount,
                entry.denseRankPower,
                entry.numValues
            );
            return new NumericDocValues() {

                private final ES97TSDBDocValuesEncoder decoder = new ES97TSDBDocValuesEncoder();
                private long currentBlockIndex = -1;
                private final long[] currentBlock = new long[ES97TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE];

                @Override
                public int advance(int target) throws IOException {
                    return disi.advance(target);
                }

                @Override
                public boolean advanceExact(int target) throws IOException {
                    return disi.advanceExact(target);
                }

                @Override
                public int nextDoc() throws IOException {
                    return disi.nextDoc();
                }

                @Override
                public int docID() {
                    return disi.docID();
                }

                @Override
                public long cost() {
                    return disi.cost();
                }

                @Override
                public long longValue() throws IOException {
                    final int index = disi.index();
                    final int blockIndex = index >>> ES97TSDBDocValuesFormat.NUMERIC_BLOCK_SHIFT;
                    final int blockInIndex = index & ES97TSDBDocValuesFormat.NUMERIC_BLOCK_MASK;
                    if (blockIndex != currentBlockIndex) {
                        assert blockIndex > currentBlockIndex;
                        if (blockIndex - 1 > currentBlockIndex) {
                            valuesData.seek(indexReader.get(blockIndex));
                        }
                        currentBlockIndex = blockIndex;
                        decoder.decode(valuesData, currentBlock);
                    }
                    return currentBlock[blockInIndex];
                }
            };
        }
    }

}
