/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.lucene.bwc.codecs;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Version;
import org.elasticsearch.xpack.lucene.bwc.codecs.lucene80.BWCLucene80Codec;
import org.elasticsearch.xpack.lucene.bwc.codecs.lucene84.BWCLucene84Codec;
import org.elasticsearch.xpack.lucene.bwc.codecs.lucene86.BWCLucene86Codec;
import org.elasticsearch.xpack.lucene.bwc.codecs.lucene87.BWCLucene87Codec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Base class for older BWC codecs
 */
public abstract class BWCCodec extends Codec {

    protected FieldInfosFormat fieldInfosFormat;
    protected SegmentInfoFormat segmentInfosFormat;

    protected BWCCodec(String name) {
        super(name);
        this.fieldInfosFormat = wrappedFieldInfosFormat();
        this.segmentInfosFormat = wrappedSegmentInfoFormat();
    }

    protected final PostingsFormat postingsFormat = new PerFieldPostingsFormat() {
        @Override
        public PostingsFormat getPostingsFormatForField(String field) {
            throw new UnsupportedOperationException("Old codecs can't be used for writing");
        }
    };

    @Override
    public final NormsFormat normsFormat() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final TermVectorsFormat termVectorsFormat() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final KnnVectorsFormat knnVectorsFormat() {
        throw new UnsupportedOperationException();
    }

    protected abstract SegmentInfoFormat setSegmentInfoFormat();

    protected final SegmentInfoFormat wrappedSegmentInfoFormat() {
        return new SegmentInfoFormat() {
            final SegmentInfoFormat wrappedFormat = setSegmentInfoFormat();

            @Override
            public SegmentInfo read(Directory directory, String segmentName, byte[] segmentID, IOContext context) throws IOException {
                return wrap(wrappedFormat.read(directory, segmentName, segmentID, context));
            }

            @Override
            public void write(Directory dir, SegmentInfo info, IOContext ioContext) throws IOException {
                wrappedFormat.write(dir, info, ioContext);
            }
        };
    }

    protected abstract FieldInfosFormat setFieldInfosFormat();

    protected final FieldInfosFormat wrappedFieldInfosFormat() {
        return new FieldInfosFormat() {
            final FieldInfosFormat wrappedFormat = setFieldInfosFormat();

            @Override
            public FieldInfos read(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, IOContext iocontext)
                throws IOException {
                return filterFields(wrappedFormat.read(directory, segmentInfo, segmentSuffix, iocontext));
            }

            @Override
            public void write(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, FieldInfos infos, IOContext context)
                throws IOException {
                wrappedFormat.write(directory, segmentInfo, segmentSuffix, infos, context);
            }
        };
    }

    // mark all fields as no term vectors, no norms, no payloads, and no vectors.
    private static FieldInfos filterFields(FieldInfos fieldInfos) {
        List<FieldInfo> fieldInfoCopy = new ArrayList<>(fieldInfos.size());
        for (FieldInfo fieldInfo : fieldInfos) {
            fieldInfoCopy.add(
                new FieldInfo(
                    fieldInfo.name,
                    fieldInfo.number,
                    false,
                    true,
                    false,
                    fieldInfo.getIndexOptions(),
                    fieldInfo.getDocValuesType(),
                    fieldInfo.docValuesSkipIndexType(),
                    fieldInfo.getDocValuesGen(),
                    fieldInfo.attributes(),
                    fieldInfo.getPointDimensionCount(),
                    fieldInfo.getPointIndexDimensionCount(),
                    fieldInfo.getPointNumBytes(),
                    0,
                    fieldInfo.getVectorEncoding(),
                    fieldInfo.getVectorSimilarityFunction(),
                    fieldInfo.isSoftDeletesField(),
                    fieldInfo.isParentField()
                )
            );
        }
        FieldInfos newFieldInfos = new FieldInfos(fieldInfoCopy.toArray(new FieldInfo[0]));
        return newFieldInfos;
    }

    public static SegmentInfo wrap(SegmentInfo segmentInfo) {
        Codec codec = getBackwardCompatibleCodec(segmentInfo.getCodec());

        final SegmentInfo segmentInfo1 = new SegmentInfo(
            segmentInfo.dir,
            // Use Version.LATEST instead of original version, otherwise SegmentCommitInfo will bark when processing (N-1 limitation)
            // TODO: perhaps store the original version information in attributes so that we can retrieve it later when needed?
            Version.LATEST,
            Version.LATEST,
            segmentInfo.name,
            segmentInfo.maxDoc(),
            segmentInfo.getUseCompoundFile(),
            segmentInfo.getHasBlocks(),
            codec,
            segmentInfo.getDiagnostics(),
            segmentInfo.getId(),
            segmentInfo.getAttributes(),
            segmentInfo.getIndexSort()
        );
        segmentInfo1.setFiles(segmentInfo.files());
        return segmentInfo1;
    }

    // Special handling for Lucene8xCodecs (which are currently bundled with Lucene)
    // Use BWCLucene8xCodec instead as that one extends BWCCodec (similar to all other older codecs)
    private static Codec getBackwardCompatibleCodec(Codec codec) {
        if (codec == null) return null;

        return switch (codec.getClass().getSimpleName()) {
            case "Lucene80Codec" -> new BWCLucene80Codec();
            case "Lucene84Codec" -> new BWCLucene84Codec();
            case "Lucene86Codec" -> new BWCLucene86Codec();
            case "Lucene87Codec" -> new BWCLucene87Codec();
            default -> codec;
        };
    }

    /**
     * In-memory postings format that shows no postings available.
     */
    public static class EmptyPostingsFormat extends PostingsFormat {

        public EmptyPostingsFormat() {
            super("EmptyPostingsFormat");
        }

        @Override
        public FieldsConsumer fieldsConsumer(SegmentWriteState state) {
            return new FieldsConsumer() {
                @Override
                public void write(Fields fields, NormsProducer norms) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void close() {

                }
            };
        }

        @Override
        public FieldsProducer fieldsProducer(SegmentReadState state) {
            return new FieldsProducer() {
                @Override
                public void close() {

                }

                @Override
                public void checkIntegrity() {

                }

                @Override
                public Iterator<String> iterator() {
                    return null;
                }

                @Override
                public Terms terms(String field) {
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
