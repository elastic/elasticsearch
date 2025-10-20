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
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.index.mapper.SyntheticIdField;

import java.io.IOException;
import java.util.HashMap;

import static org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat.SYNTHETIC_ID;
import static org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat.TIMESTAMP;
import static org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat.TS_ID;

/**
 * Special codec for time-series Lucene indices that use synthetic ids.
 * <p>
 *     This codec ensures that the synthetic _id field does not have index options that would build an inverted index for the field during
 *     indexing. It also reverts this index options when reading FieldInfos so that it can use postings for documents soft-updates and other
 *     APIs.
 */
public class TSDBSyntheticIdCodec extends FilterCodec {

    private final TSDBSyntheticIdFieldInfosFormat fieldInfosFormat;

    public TSDBSyntheticIdCodec(String name, Codec delegate) {
        super(name, delegate);
        this.fieldInfosFormat = new TSDBSyntheticIdFieldInfosFormat(delegate.fieldInfosFormat());
    }

    @Override
    public final FieldInfosFormat fieldInfosFormat() {
        return fieldInfosFormat;
    }

    /**
     * {@link FieldInfosFormat} that ensures the _id field is synthetic
     */
    private static class TSDBSyntheticIdFieldInfosFormat extends FieldInfosFormat {

        private final FieldInfosFormat delegate;

        private TSDBSyntheticIdFieldInfosFormat(FieldInfosFormat delegate) {
            this.delegate = delegate;
        }

        private FieldInfo safeIdFieldInfo(FieldInfos fieldInfos) {
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
            // Ensure _id exists and not indexed
            fi = fieldInfos.fieldInfo(SYNTHETIC_ID);
            if (fi == null) {
                var message = "Field [" + SYNTHETIC_ID + "] does not exist";
                assert false : message;
                throw new IllegalArgumentException(message);
            }
            if (fi.getIndexOptions() != IndexOptions.NONE) {
                assert false;
                throw new IllegalArgumentException("Field [" + SYNTHETIC_ID + "] has incorrect index options");
            }
            return fi;
        }

        @Override
        public void write(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, FieldInfos fieldInfos, IOContext context)
            throws IOException {

            final var fieldInfo = safeIdFieldInfo(fieldInfos);
            if (SyntheticIdField.hasSyntheticIdAttributes(fieldInfo.attributes()) == false) {
                throw new IllegalArgumentException("Field [" + SYNTHETIC_ID + "] is not synthetic");
            }
            delegate.write(directory, segmentInfo, segmentSuffix, fieldInfos, context);
        }

        @Override
        public FieldInfos read(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, IOContext iocontext) throws IOException {
            final var fieldInfos = delegate.read(directory, segmentInfo, segmentSuffix, iocontext);

            final var fieldInfo = safeIdFieldInfo(fieldInfos);
            if (SyntheticIdField.hasSyntheticIdAttributes(fieldInfo.attributes()) == false) {
                throw new IllegalArgumentException("Field [" + SYNTHETIC_ID + "] is not synthetic");
            }

            // Change the _id field index options from IndexOptions.NONE to IndexOptions.DOCS, so that terms and postings work when
            // applying doc values updates in Lucene.
            final var infos = new FieldInfo[fieldInfos.size()];
            int i = 0;
            for (FieldInfo fi : fieldInfos) {
                if (SYNTHETIC_ID.equals(fi.getName())) {
                    assert fieldInfo.attributes().containsKey(PerFieldPostingsFormat.PER_FIELD_SUFFIX_KEY) == false;
                    assert fieldInfo.attributes().containsKey(PerFieldPostingsFormat.PER_FIELD_SUFFIX_KEY) == false;

                    var attributes = new HashMap<>(fieldInfo.attributes());
                    attributes.put(PerFieldPostingsFormat.PER_FIELD_FORMAT_KEY, TSDBSyntheticIdPostingsFormat.FORMAT_NAME);
                    attributes.put(PerFieldPostingsFormat.PER_FIELD_SUFFIX_KEY, "0");

                    fi = new FieldInfo(
                        fi.getName(),
                        fi.getFieldNumber(),
                        fi.hasTermVectors(),
                        true,
                        fi.hasPayloads(),
                        IndexOptions.DOCS,
                        fi.getDocValuesType(),
                        fi.docValuesSkipIndexType(),
                        fi.getDocValuesGen(),
                        attributes,
                        fi.getPointDimensionCount(),
                        fi.getPointIndexDimensionCount(),
                        fi.getPointNumBytes(),
                        fi.getVectorDimension(),
                        fi.getVectorEncoding(),
                        fi.getVectorSimilarityFunction(),
                        fi.isSoftDeletesField(),
                        fi.isParentField()
                    );
                }
                infos[i++] = fi;
            }
            return new FieldInfos(infos);
        }
    }
}
