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
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdStoredFieldsReader;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.SyntheticIdField;

import java.io.Closeable;
import java.io.IOException;

/**
 * Composite stored fields format for {@code TIME_SERIES} indices that combines synthetic id materialization with standard field storage.
 */
public class TSDBStoredFieldsFormat extends StoredFieldsFormat {
    private final StoredFieldsFormat delegate;

    public TSDBStoredFieldsFormat(StoredFieldsFormat delegate) {
        this.delegate = delegate;
    }

    @Override
    public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
        return new TSDBStoredFieldsReader(directory, si, fn, context);
    }

    @Override
    public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) throws IOException {
        return delegate.fieldsWriter(directory, si, context);
    }

    class TSDBStoredFieldsReader extends StoredFieldsReader {

        private final StoredFieldsReader storedFieldsReader;
        private final @Nullable StoredFieldsReader syntheticIdStoredFieldsReader; // null if no synthetic _id

        TSDBStoredFieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
            Closeable closeable = null;
            try {
                this.storedFieldsReader = delegate.fieldsReader(directory, si, fn, context);
                closeable = this.storedFieldsReader;
                var fieldInfo = fn.fieldInfo(IdFieldMapper.NAME);
                if (fieldInfo != null && SyntheticIdField.hasSyntheticIdAttributes(fieldInfo.attributes())) {
                    this.syntheticIdStoredFieldsReader = TSDBSyntheticIdStoredFieldsReader.open(directory, si, fn, context);
                } else {
                    this.syntheticIdStoredFieldsReader = null;
                }
                closeable = null;
            } finally {
                IOUtils.close(closeable);
            }
        }

        TSDBStoredFieldsReader(StoredFieldsReader storedFieldsReader, @Nullable StoredFieldsReader syntheticIdStoredFieldsReader) {
            this.storedFieldsReader = storedFieldsReader;
            assert syntheticIdStoredFieldsReader == null || syntheticIdStoredFieldsReader instanceof TSDBSyntheticIdStoredFieldsReader;
            this.syntheticIdStoredFieldsReader = syntheticIdStoredFieldsReader;
        }

        @Override
        public StoredFieldsReader clone() {
            return new TSDBStoredFieldsReader(
                storedFieldsReader.clone(),
                syntheticIdStoredFieldsReader != null ? syntheticIdStoredFieldsReader.clone() : null
            );
        }

        @Override
        public StoredFieldsReader getMergeInstance() {
            return new TSDBStoredFieldsReader(
                storedFieldsReader.getMergeInstance(),
                syntheticIdStoredFieldsReader != null ? syntheticIdStoredFieldsReader.getMergeInstance() : null
            );
        }

        @Override
        public void checkIntegrity() throws IOException {
            storedFieldsReader.checkIntegrity();
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(storedFieldsReader, syntheticIdStoredFieldsReader);
        }

        @Override
        public void document(int docID, StoredFieldVisitor visitor) throws IOException {
            // Some clients of this API expect that the _id is read before other fields,
            // therefore we call first to the bloom filter reader so we can synthesize the _id
            // and read it in the expected order.
            if (syntheticIdStoredFieldsReader != null) {
                syntheticIdStoredFieldsReader.document(docID, visitor);
            }
            storedFieldsReader.document(docID, visitor);
        }
    }
}
