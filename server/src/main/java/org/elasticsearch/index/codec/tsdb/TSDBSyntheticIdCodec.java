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
import static org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat.TS_ROUTING_HASH;

/**
 * Special codec for time-series datastreams that use synthetic ids.
 * <p>
 *     The role of this codec is to ensure that no inverted index is created when indexing a document id in Lucene, while allowing the usage
 *     of terms and postings on the field (now called a "synthetic _id" field) as if it was backed by an in inverted index.
 * </p>
 * <p>
 *     In order to do this, it wraps the default postings format with an implementation that throws an {@link IllegalArgumentException} if
 *     a Lucene field with the name {@code _id} produces terms (ie, has postings) during indexing. It also overwrites the {@link FieldInfos}
 *     to ensure that the {@code _id} field information has the {@link IndexOptions#NONE} option when written to disk. It  also changes this
 *     {@link IndexOptions#NONE} option back to {@link IndexOptions#DOCS} when reading the {@link FieldInfos} during the opening of a new
 *     segment core reader. This allows to use a Lucene term dictionary on top of a synthetic _id field that does not have corresponding
 *     postings files on disk. Finally, the codec injects additional {@link FieldInfos} attributes so that Lucene's
 *     {@link PerFieldPostingsFormat} correctly instantiates a {@link TSDBSyntheticIdPostingsFormat} to access the term and postings of the
 *     synthetic _id field.
 * </p>
 */
public class TSDBSyntheticIdCodec extends FilterCodec {

    private final RewriteFieldInfosFormat fieldInfosFormat;

    public TSDBSyntheticIdCodec(Codec delegate) {
        super(delegate.getName(), delegate);
        this.fieldInfosFormat = new RewriteFieldInfosFormat(delegate.fieldInfosFormat());
    }

    @Override
    public final FieldInfosFormat fieldInfosFormat() {
        return fieldInfosFormat;
    }

    /**
     * {@link FieldInfosFormat} that overwrites the {@link FieldInfos}.
     */
    private static class RewriteFieldInfosFormat extends FieldInfosFormat {

        private final FieldInfosFormat delegate;

        private RewriteFieldInfosFormat(FieldInfosFormat delegate) {
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
            if (fi.getIndexOptions() != IndexOptions.NONE) {
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

            // Change the _id field index options from IndexOptions.DOCS to IndexOptions.NONE
            final var infos = new FieldInfo[fieldInfos.size()];
            int i = 0;
            for (FieldInfo fi : fieldInfos) {
                if (SYNTHETIC_ID.equals(fi.getName())) {
                    final var attributes = new HashMap<>(fi.attributes());

                    // Assert that PerFieldPostingsFormat are not present or have the expected format and suffix
                    assert attributes.get(PerFieldPostingsFormat.PER_FIELD_FORMAT_KEY) == null
                        || TSDBSyntheticIdPostingsFormat.FORMAT_NAME.equals(attributes.get(PerFieldPostingsFormat.PER_FIELD_FORMAT_KEY));
                    assert attributes.get(PerFieldPostingsFormat.PER_FIELD_SUFFIX_KEY) == null
                        || TSDBSyntheticIdPostingsFormat.SUFFIX.equals(attributes.get(PerFieldPostingsFormat.PER_FIELD_SUFFIX_KEY));

                    // Remove attributes if present
                    attributes.remove(PerFieldPostingsFormat.PER_FIELD_FORMAT_KEY);
                    attributes.remove(PerFieldPostingsFormat.PER_FIELD_SUFFIX_KEY);

                    fi = new FieldInfo(
                        fi.getName(),
                        fi.getFieldNumber(),
                        fi.hasTermVectors(),
                        true,
                        fi.hasPayloads(),
                        IndexOptions.NONE,
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

            fieldInfos = new FieldInfos(infos);
            ensureSyntheticIdFields(fieldInfos);
            delegate.write(directory, segmentInfo, segmentSuffix, fieldInfos, context);
        }

        @Override
        public FieldInfos read(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, IOContext iocontext) throws IOException {
            final var fieldInfos = delegate.read(directory, segmentInfo, segmentSuffix, iocontext);
            ensureSyntheticIdFields(fieldInfos);

            // Change the _id field index options from IndexOptions.NONE to IndexOptions.DOCS, so that terms and postings work when
            // applying doc values updates in Lucene.
            final var infos = new FieldInfo[fieldInfos.size()];
            int i = 0;
            for (FieldInfo fi : fieldInfos) {
                if (SYNTHETIC_ID.equals(fi.getName())) {
                    final var attributes = new HashMap<>(fi.attributes());

                    // Assert that PerFieldPostingsFormat are not written to field infos on disk
                    assert attributes.containsKey(PerFieldPostingsFormat.PER_FIELD_FORMAT_KEY) == false;
                    assert attributes.containsKey(PerFieldPostingsFormat.PER_FIELD_SUFFIX_KEY) == false;

                    // Inject attributes so that PerFieldPostingsFormat maps the synthetic _id field to the TSDBSyntheticIdPostingsFormat
                    // This would normally be handled transparently by PerFieldPostingsFormat, but such attributes are only added if terms
                    // are produced during indexing, which is not the case for the synthetic _id field.
                    attributes.put(PerFieldPostingsFormat.PER_FIELD_FORMAT_KEY, TSDBSyntheticIdPostingsFormat.FORMAT_NAME);
                    attributes.put(PerFieldPostingsFormat.PER_FIELD_SUFFIX_KEY, TSDBSyntheticIdPostingsFormat.SUFFIX);

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
