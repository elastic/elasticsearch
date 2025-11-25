/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.codec.bloomfilter.ES93BloomFilterStoredFieldsFormat;
import org.elasticsearch.index.codec.storedfields.TSDBStoredFieldsFormat;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.SyntheticIdField;

import java.io.IOException;

import static org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat.SYNTHETIC_ID;
import static org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat.TIMESTAMP;
import static org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat.TS_ID;
import static org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat.TS_ROUTING_HASH;

/**
 * Abstract base class for ES codecs used with time-series ({@code TIME_SERIES}) indices
 * that employ synthetic document IDs for storage optimization.
 *
 * <p>This class configures the codec to use the following formats:
 * <ul>
 *   <li>Apply {@link TSDBStoredFieldsFormat} with bloom filter optimization for efficient ID lookups</li>
 * </ul>
 *
 * <p>
 *     Synthetic IDs in TSDB indices are generated from the document's dimensions and timestamp,
 *     replacing the standard {@code _id} field to reduce storage overhead.
 *
 * <p>
 *     One of the roles of this codec is to ensure that no inverted index is created when indexing a document id in Lucene,
 *     while allowing the usage of terms and postings on the field (now called a "synthetic _id" field) as if it was backed
 *     by an in inverted index.
 * </p>
 *
 * <p>
 *     Additionally, validates that all required fields are present and properly structured within the segment.
 * </p>
 *
 * @see TSDBStoredFieldsFormat
 */
abstract class AbstractTSDBSyntheticIdCodec extends FilterCodec {
    private final TSDBStoredFieldsFormat storedFieldsFormat;
    private final ValidatingFieldInfosFormat fieldInfosFormat;
    private final EnsureNoPostingsFormat postingsFormat;

    AbstractTSDBSyntheticIdCodec(String name, Codec delegate, BigArrays bigArrays) {
        super(name, delegate);
        this.storedFieldsFormat = new TSDBStoredFieldsFormat(
            delegate.storedFieldsFormat(),
            new ES93BloomFilterStoredFieldsFormat(
                bigArrays,
                ES93BloomFilterStoredFieldsFormat.DEFAULT_BLOOM_FILTER_SIZE,
                IdFieldMapper.NAME
            )
        );
        this.fieldInfosFormat = new ValidatingFieldInfosFormat(delegate.fieldInfosFormat());
        this.postingsFormat = new EnsureNoPostingsFormat(delegate.postingsFormat());
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return storedFieldsFormat;
    }

    @Override
    public final FieldInfosFormat fieldInfosFormat() {
        return fieldInfosFormat;
    }

    @Override
    public PostingsFormat postingsFormat() {
        return postingsFormat;
    }

    private static class ValidatingFieldInfosFormat extends FieldInfosFormat {

        private final FieldInfosFormat delegate;

        private ValidatingFieldInfosFormat(FieldInfosFormat delegate) {
            this.delegate = delegate;
        }

        private void ensureSyntheticIdFields(FieldInfos fieldInfos) {
            // Ensure _tsid exists
            var fi = fieldInfos.fieldInfo(TS_ID);
            if (fi == null) {
                var message = "Field [" + TS_ID + "] does not exist";
                assert false : message;
                throw new IllegalArgumentException(message);
            }
            // Ensure @timestamp exists
            fi = fieldInfos.fieldInfo(TIMESTAMP);
            if (fi == null) {
                var message = "Field [" + TIMESTAMP + "] does not exist";
                assert false : message;
                throw new IllegalArgumentException(message);
            }
            // Ensure _ts_routing_hash exists
            fi = fieldInfos.fieldInfo(TS_ROUTING_HASH);
            if (fi == null) {
                var message = "Field [" + TS_ROUTING_HASH + "] does not exist";
                assert false : message;
                throw new IllegalArgumentException(message);
            }
            // Ensure _id exists and not indexed
            fi = fieldInfos.fieldInfo(SYNTHETIC_ID);
            if (fi == null) {
                var message = "Field [" + SYNTHETIC_ID + "] does not exist";
                assert false : message;
                throw new IllegalArgumentException(message);
            }
            if (fi.getIndexOptions() != IndexOptions.DOCS) {
                assert false;
                throw new IllegalArgumentException("Field [" + SYNTHETIC_ID + "] has incorrect index options");
            }
            if (SyntheticIdField.hasSyntheticIdAttributes(fi.attributes()) == false) {
                throw new IllegalArgumentException("Field [" + SYNTHETIC_ID + "] is not synthetic");
            }
        }

        @Override
        public void write(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, FieldInfos fieldInfos, IOContext context)
            throws IOException {
            ensureSyntheticIdFields(fieldInfos);
            delegate.write(directory, segmentInfo, segmentSuffix, fieldInfos, context);
        }

        @Override
        public FieldInfos read(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, IOContext iocontext) throws IOException {
            final var fieldInfos = delegate.read(directory, segmentInfo, segmentSuffix, iocontext);
            ensureSyntheticIdFields(fieldInfos);
            return fieldInfos;
        }
    }

    /**
     * {@link PostingsFormat} that throws an {@link IllegalArgumentException} if a Lucene field with the name {@code _id} has postings
     * produced during indexing.
     */
    private static class EnsureNoPostingsFormat extends PostingsFormat {

        private final PostingsFormat delegate;

        private EnsureNoPostingsFormat(PostingsFormat delegate) {
            super(delegate.getName());
            this.delegate = delegate;
        }

        @Override
        public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
            final var consumer = delegate.fieldsConsumer(state);
            return new FieldsConsumer() {
                @Override
                public void write(Fields fields, NormsProducer norms) throws IOException {
                    for (var field : fields) {
                        if (SYNTHETIC_ID.equalsIgnoreCase(field)) {
                            var message = "Field [" + SYNTHETIC_ID + "] has terms produced during indexing";
                            assert false : message;
                            throw new IllegalArgumentException(message);
                        }
                    }
                    consumer.write(fields, norms);
                }

                @Override
                public void close() throws IOException {
                    consumer.close();
                }
            };
        }

        @Override
        public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
            return delegate.fieldsProducer(state);
        }
    }
}
