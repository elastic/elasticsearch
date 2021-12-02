/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.lucene.bwc;

import org.apache.lucene.backward_codecs.lucene50.Lucene50CompoundFormat;
import org.apache.lucene.backward_codecs.lucene50.Lucene50LiveDocsFormat;
import org.apache.lucene.backward_codecs.lucene50.Lucene50StoredFieldsFormat;
import org.apache.lucene.backward_codecs.lucene60.Lucene60FieldInfosFormat;
import org.apache.lucene.backward_codecs.lucene70.Lucene70SegmentInfoFormat;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BWCLucene70Codec extends Codec {

    private final FieldInfosFormat fieldInfosFormat;
    private final SegmentInfoFormat segmentInfosFormat;
    private final LiveDocsFormat liveDocsFormat;
    private final CompoundFormat compoundFormat;
    private final PostingsFormat postingsFormat;
    private final StoredFieldsFormat storedFieldsFormat;

    public BWCLucene70Codec() {
        super("BWCLucene70Codec");
        fieldInfosFormat = wrap(new Lucene60FieldInfosFormat());
        segmentInfosFormat = wrap(new Lucene70SegmentInfoFormat());
        liveDocsFormat = new Lucene50LiveDocsFormat();
        compoundFormat = new Lucene50CompoundFormat();
        postingsFormat = new EmptyPostingsFormat();
        storedFieldsFormat = new Lucene50StoredFieldsFormat(Lucene50StoredFieldsFormat.Mode.BEST_SPEED);
    }

    @Override
    public FieldInfosFormat fieldInfosFormat() {
        return fieldInfosFormat;
    }

    @Override
    public SegmentInfoFormat segmentInfoFormat() {
        return segmentInfosFormat;
    }

    @Override
    public NormsFormat normsFormat() {
        throw new UnsupportedOperationException();
    }

    @Override
    public PostingsFormat postingsFormat() {
        return postingsFormat;
    }

    @Override
    public DocValuesFormat docValuesFormat() {
        throw new UnsupportedOperationException();
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return storedFieldsFormat;
    }

    @Override
    public TermVectorsFormat termVectorsFormat() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LiveDocsFormat liveDocsFormat() {
        return liveDocsFormat;
    }

    @Override
    public CompoundFormat compoundFormat() {
        return compoundFormat;
    }

    @Override
    public PointsFormat pointsFormat() {
        throw new UnsupportedOperationException();
    }

    @Override
    public KnnVectorsFormat knnVectorsFormat() {
        throw new UnsupportedOperationException();
    }

    private static SegmentInfoFormat wrap(SegmentInfoFormat wrapped) {
        return new SegmentInfoFormat() {
            @Override
            public SegmentInfo read(Directory directory, String segmentName, byte[] segmentID, IOContext context) throws IOException {
                return adaptVersion(wrapped.read(directory, segmentName, segmentID, context));
            }

            @Override
            public void write(Directory dir, SegmentInfo info, IOContext ioContext) throws IOException {
                wrapped.write(dir, info, ioContext);
            }
        };
    }

    private static FieldInfosFormat wrap(FieldInfosFormat wrapped) {
        return new FieldInfosFormat() {
            @Override
            public FieldInfos read(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, IOContext iocontext)
                throws IOException {
                return filterFields(wrapped.read(directory, segmentInfo, segmentSuffix, iocontext));
            }

            @Override
            public void write(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, FieldInfos infos, IOContext context)
                throws IOException {
                wrapped.write(directory, segmentInfo, segmentSuffix, infos, context);
            }
        };
    }

    // mark all fields as having no postings, no doc values, and no points.
    private static FieldInfos filterFields(FieldInfos fieldInfos) {
        List<FieldInfo> fieldInfoCopy = new ArrayList<>(fieldInfos.size());
        for (FieldInfo fieldInfo : fieldInfos) {
            fieldInfoCopy.add(
                new FieldInfo(
                    fieldInfo.name,
                    fieldInfo.number,
                    false,
                    false,
                    false,
                    IndexOptions.NONE,
                    DocValuesType.NONE,
                    -1,
                    fieldInfo.attributes(),
                    0,
                    0,
                    0,
                    0,
                    fieldInfo.getVectorSimilarityFunction(),
                    fieldInfo.isSoftDeletesField()
                )
            );
        }
        FieldInfos newFieldInfos = new FieldInfos(fieldInfoCopy.toArray(new FieldInfo[0]));
        return newFieldInfos;
    }

    private static SegmentInfo adaptVersion(SegmentInfo segmentInfo) {
        return OldLuceneVersions.wrap(segmentInfo);
    }

    static class EmptyPostingsFormat extends PostingsFormat {

        protected EmptyPostingsFormat() {
            super("EmptyPostingsFormat");
        }

        @Override
        public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
            return new FieldsConsumer() {
                @Override
                public void write(Fields fields, NormsProducer norms) throws IOException {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void close() throws IOException {

                }
            };
        }

        @Override
        public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
            return new FieldsProducer() {
                @Override
                public void close() throws IOException {

                }

                @Override
                public void checkIntegrity() throws IOException {

                }

                @Override
                public Iterator<String> iterator() {
                    return null;
                }

                @Override
                public Terms terms(String field) throws IOException {
                    return null;
                }

                @Override
                public int size() {
                    return 0;
                }
            };
        }
    }
}
