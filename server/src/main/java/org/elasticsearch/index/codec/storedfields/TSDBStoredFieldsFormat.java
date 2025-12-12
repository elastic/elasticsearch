/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.storedfields;

import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.codec.bloomfilter.BloomFilter;
import org.elasticsearch.index.codec.bloomfilter.ES93BloomFilterStoredFieldsFormat;
import org.elasticsearch.index.mapper.IdFieldMapper;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Composite stored fields format for {@code TIME_SERIES} indices that combines bloom filter optimization
 * for document ID lookups with standard field storage.
 *
 * <p>This format uses a two-layer approach:
 * <ul>
 *   <li>{@link ES93BloomFilterStoredFieldsFormat} - Creates a bloom filter index on the {@code _id}
 *       field to enable fast document existence checks and skips storing the _id</li>
 *   <li>Delegate {@link StoredFieldsFormat} - Handles storage and retrieval of all other fields
 *       using the standard format</li>
 * </ul>
 *
 * @see ES93BloomFilterStoredFieldsFormat
 * @see StoredFieldsFormat
 */
public class TSDBStoredFieldsFormat extends StoredFieldsFormat {
    private final StoredFieldsFormat delegate;
    private final ES93BloomFilterStoredFieldsFormat bloomFilterStoredFieldsFormat;

    public TSDBStoredFieldsFormat(StoredFieldsFormat delegate, ES93BloomFilterStoredFieldsFormat bloomFilterStoredFieldsFormat) {
        this.delegate = delegate;
        this.bloomFilterStoredFieldsFormat = bloomFilterStoredFieldsFormat;
    }

    @Override
    public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
        return new TSDBStoredFieldsReader(directory, si, fn, context);
    }

    @Override
    public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) throws IOException {
        return new TSDBStoredFieldsWriter(directory, si, context);
    }

    class TSDBStoredFieldsWriter extends StoredFieldsWriter {
        private final StoredFieldsWriter storedFieldsWriter;
        private final StoredFieldsWriter bloomFilterStoredFieldsWriter;
        private int idFieldNumber = Integer.MIN_VALUE;

        TSDBStoredFieldsWriter(Directory directory, SegmentInfo si, IOContext context) throws IOException {
            boolean success = false;
            List<Closeable> toClose = new ArrayList<>(2);
            try {
                this.storedFieldsWriter = delegate.fieldsWriter(directory, si, context);
                toClose.add(storedFieldsWriter);
                this.bloomFilterStoredFieldsWriter = bloomFilterStoredFieldsFormat.fieldsWriter(directory, si, context);
                toClose.add(bloomFilterStoredFieldsWriter);
                success = true;
            } finally {
                if (success == false) {
                    IOUtils.close(toClose);
                }
            }
        }

        @Override
        public void startDocument() throws IOException {
            storedFieldsWriter.startDocument();
            bloomFilterStoredFieldsWriter.startDocument();
        }

        @Override
        public void finishDocument() throws IOException {
            storedFieldsWriter.finishDocument();
            bloomFilterStoredFieldsWriter.finishDocument();
        }

        @Override
        public void writeField(FieldInfo info, int value) throws IOException {
            getWriterForField(info).writeField(info, value);
        }

        @Override
        public void writeField(FieldInfo info, long value) throws IOException {
            getWriterForField(info).writeField(info, value);
        }

        @Override
        public void writeField(FieldInfo info, float value) throws IOException {
            getWriterForField(info).writeField(info, value);
        }

        @Override
        public void writeField(FieldInfo info, double value) throws IOException {
            getWriterForField(info).writeField(info, value);
        }

        @Override
        public void writeField(FieldInfo info, BytesRef value) throws IOException {
            getWriterForField(info).writeField(info, value);
        }

        @Override
        public void writeField(FieldInfo info, String value) throws IOException {
            getWriterForField(info).writeField(info, value);
        }

        @Override
        public void finish(int numDocs) throws IOException {
            storedFieldsWriter.finish(numDocs);
            bloomFilterStoredFieldsWriter.finish(numDocs);
        }

        @Override
        public int merge(MergeState mergeState) throws IOException {
            var totalDocs = 0;
            totalDocs += storedFieldsWriter.merge(unwrapStoredFieldReaders(mergeState, false));
            totalDocs += bloomFilterStoredFieldsWriter.merge(unwrapStoredFieldReaders(mergeState, true));
            return totalDocs;
        }

        private MergeState unwrapStoredFieldReaders(MergeState mergeState, boolean unwrapBloomFilterReaders) {
            StoredFieldsReader[] updatedReaders = new StoredFieldsReader[mergeState.storedFieldsReaders.length];
            for (int i = 0; i < mergeState.storedFieldsReaders.length; i++) {
                final StoredFieldsReader storedFieldsReader = mergeState.storedFieldsReaders[i];

                // We need to unwrap the stored field readers belonging to a PerFieldStoredFieldsFormat,
                // otherwise, downstream formats won't be able to perform certain optimizations when
                // they try to merge segments as they expect an instance of the actual Reader in their checks
                // (i.e. Lucene90CompressingStoredFieldsReader would do chunk merging for instances of the same class)
                if (storedFieldsReader instanceof TSDBStoredFieldsReader reader) {
                    // In case that we're dealing with a previous format, the newer formats should be able to handle it
                    updatedReaders[i] = unwrapBloomFilterReaders ? reader.bloomFilterStoredFieldsReader : reader.storedFieldsReader;
                } else {
                    updatedReaders[i] = storedFieldsReader;
                }
            }

            return new MergeState(
                mergeState.docMaps,
                mergeState.segmentInfo,
                mergeState.mergeFieldInfos,
                updatedReaders,
                mergeState.termVectorsReaders,
                mergeState.normsProducers,
                mergeState.docValuesProducers,
                mergeState.fieldInfos,
                mergeState.liveDocs,
                mergeState.fieldsProducers,
                mergeState.pointsReaders,
                mergeState.knnVectorsReaders,
                mergeState.maxDocs,
                mergeState.infoStream,
                mergeState.intraMergeTaskExecutor,
                mergeState.needsIndexSort
            );
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(storedFieldsWriter, bloomFilterStoredFieldsWriter);
        }

        @Override
        public long ramBytesUsed() {
            return storedFieldsWriter.ramBytesUsed() + bloomFilterStoredFieldsWriter.ramBytesUsed();
        }

        private StoredFieldsWriter getWriterForField(FieldInfo field) {
            if (field.number == idFieldNumber || field.name.equals(IdFieldMapper.NAME)) {
                idFieldNumber = field.number;
                return bloomFilterStoredFieldsWriter;
            }
            return storedFieldsWriter;
        }
    }

    class TSDBStoredFieldsReader extends StoredFieldsReader implements BloomFilter {
        private final StoredFieldsReader storedFieldsReader;
        private final StoredFieldsReader bloomFilterStoredFieldsReader;
        private final BloomFilter bloomFilter;

        TSDBStoredFieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
            boolean success = false;
            List<Closeable> toClose = new ArrayList<>(2);
            try {
                this.storedFieldsReader = delegate.fieldsReader(directory, si, fn, context);
                toClose.add(this.storedFieldsReader);
                this.bloomFilterStoredFieldsReader = bloomFilterStoredFieldsFormat.fieldsReader(directory, si, fn, context);
                this.bloomFilter = (BloomFilter) bloomFilterStoredFieldsReader;
                toClose.add(this.bloomFilterStoredFieldsReader);
                success = true;
            } finally {
                if (success == false) {
                    IOUtils.close(toClose);
                }
            }
        }

        TSDBStoredFieldsReader(StoredFieldsReader storedFieldsReader, StoredFieldsReader bloomFilterStoredFieldsReader) {
            this.storedFieldsReader = storedFieldsReader;
            this.bloomFilterStoredFieldsReader = bloomFilterStoredFieldsReader;
            assert bloomFilterStoredFieldsReader instanceof BloomFilter;
            this.bloomFilter = (BloomFilter) bloomFilterStoredFieldsReader;
        }

        @Override
        public StoredFieldsReader clone() {
            return new TSDBStoredFieldsReader(storedFieldsReader.clone(), bloomFilterStoredFieldsReader.clone());
        }

        @Override
        public StoredFieldsReader getMergeInstance() {
            return new TSDBStoredFieldsReader(storedFieldsReader.getMergeInstance(), bloomFilterStoredFieldsReader.getMergeInstance());
        }

        @Override
        public void checkIntegrity() throws IOException {
            storedFieldsReader.checkIntegrity();
            bloomFilterStoredFieldsReader.checkIntegrity();
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(storedFieldsReader, bloomFilterStoredFieldsReader);
        }

        @Override
        public void document(int docID, StoredFieldVisitor visitor) throws IOException {
            // Some clients of this API expect that the _id is read before other fields,
            // therefore we call first to the bloom filter reader so we can synthesize the _id
            // and read it in the expected order.
            bloomFilterStoredFieldsReader.document(docID, visitor);
            storedFieldsReader.document(docID, visitor);
        }

        @Override
        public boolean mayContainTerm(String field, BytesRef term) throws IOException {
            return bloomFilter.mayContainTerm(field, term);
        }
    }

    public static BloomFilter getBloomFilterForId(SegmentReadState state) throws IOException {
        var codec = state.segmentInfo.getCodec();
        StoredFieldsReader storedFieldsReader = codec.storedFieldsFormat()
            .fieldsReader(state.directory, state.segmentInfo, state.fieldInfos, state.context);

        if (storedFieldsReader instanceof BloomFilter bloomFilter) {
            return bloomFilter;
        } else {
            storedFieldsReader.close();
            return BloomFilter.NO_FILTER;
        }
    }
}
