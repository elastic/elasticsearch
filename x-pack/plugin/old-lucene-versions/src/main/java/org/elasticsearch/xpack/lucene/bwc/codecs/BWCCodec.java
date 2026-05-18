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
import org.elasticsearch.core.UpdateForV10;
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

    private final FieldInfosFormat fieldInfosFormat;
    private final SegmentInfoFormat segmentInfosFormat;
    private final PostingsFormat postingsFormat;

    protected BWCCodec(String name) {
        super(name);

        this.fieldInfosFormat = new FieldInfosFormat() {
            final FieldInfosFormat wrappedFormat = originalFieldInfosFormat();

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

        this.segmentInfosFormat = new SegmentInfoFormat() {
            final SegmentInfoFormat wrappedFormat = originalSegmentInfoFormat();

            @Override
            public SegmentInfo read(Directory directory, String segmentName, byte[] segmentID, IOContext context) throws IOException {
                return wrap(wrappedFormat.read(directory, segmentName, segmentID, context));
            }

            @Override
            public void write(Directory dir, SegmentInfo info, IOContext ioContext) throws IOException {
                wrappedFormat.write(dir, info, ioContext);
            }
        };

        this.postingsFormat = new PerFieldPostingsFormat() {
            @Override
            public PostingsFormat getPostingsFormatForField(String field) {
                throw new UnsupportedOperationException("Old codecs can't be used for writing");
            }
        };
    }

    @Override
    public final FieldInfosFormat fieldInfosFormat() {
        return fieldInfosFormat;
    }

    @Override
    public final SegmentInfoFormat segmentInfoFormat() {
        return segmentInfosFormat;
    }

    @Override
    public PostingsFormat postingsFormat() {
        return postingsFormat;
    }

    /**
     * This method is not supported for archive indices and older codecs and will always throw an {@link UnsupportedOperationException}.
     * This method is never called in practice, as we rewrite field infos to override the info about which features are present in
     * the index. Even if norms are present, field info lies about it.
     *
     * @return nothing, as this method always throws an exception
     * @throws UnsupportedOperationException always thrown to indicate that this method is not supported
     */
    @Override
    public final NormsFormat normsFormat() {
        throw new UnsupportedOperationException();
    }

    /**
     * This method is not supported for archive indices and older codecs and will always throw an {@link UnsupportedOperationException}.
     * This method is never called in practice, as we rewrite field infos to override the info about which features are present in
     * the index. Even if term vectors are present, field info lies about it.
     *
     * @return nothing, as this method always throws an exception
     * @throws UnsupportedOperationException always thrown to indicate that this method is not supported
     */
    @Override
    public final TermVectorsFormat termVectorsFormat() {
        throw new UnsupportedOperationException();
    }

    /**
     * This method is not supported for archive indices and older codecs and will always throw an {@link UnsupportedOperationException}.
     * The knn vectors can't be present because it is not supported yet in any of the lucene versions that we support for archive indices.
     *
     * @return nothing, as this method always throws an exception
     * @throws UnsupportedOperationException always thrown to indicate that this method is not supported
     */
    @Override
    public final KnnVectorsFormat knnVectorsFormat() {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the original {@link SegmentInfoFormat} used by this codec.
     * This method should be implemented by subclasses to provide the specific
     * {@link SegmentInfoFormat} that this codec is intended to use.
     *
     * @return the original {@link SegmentInfoFormat} used by this codec
     */
    protected abstract SegmentInfoFormat originalSegmentInfoFormat();

    /**
     * Returns the original {@link FieldInfosFormat} used by this codec.
     * This method should be implemented by subclasses to provide the specific
     * {@link FieldInfosFormat} that this codec is intended to use.
     *
     * @return the original {@link FieldInfosFormat} used by this codec
     */
    protected abstract FieldInfosFormat originalFieldInfosFormat();

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

    /**
     * Returns a backward-compatible codec for the given codec. If the codec is one of the known Lucene 8.x codecs,
     * it returns a corresponding read-only backward-compatible codec. Otherwise, it returns the original codec.
     * Lucene 8.x codecs are still shipped with the current version of Lucene.
     * Earlier codecs we are providing directly they will also be read-only backward-compatible, but they don't require the renaming.
     *
     * This switch is only for indices created in ES 6.x, later written into in ES 7.x (Lucene 8.x). Indices created
     * in ES 7.x can be read directly by ES if marked read-only, without going through archive indices.
     */
    @UpdateForV10(owner = UpdateForV10.Owner.SEARCH_FOUNDATIONS)
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
