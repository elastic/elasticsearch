/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es93;

import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.index.codec.vectors.AbstractFlatVectorsFormat;
import org.elasticsearch.index.codec.vectors.DirectIOCapableFlatVectorsFormat;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.io.IOException;
import java.util.Map;

/**
 * A generic flat format that can use several different underlying vector storage formats.
 * <p>
 * This format is not meant to be used directly; it should be used as part of another vector format implementation.
 */
public class ES93GenericFlatVectorsFormat extends AbstractFlatVectorsFormat {

    static final String NAME = "ES93GenericFlatVectorsFormat";
    static final String VECTOR_FORMAT_INFO_EXTENSION = "vfi";
    static final String META_CODEC_NAME = "ES93GenericFlatVectorsFormatMeta";

    public static final int VERSION_START = 0;
    public static final int VERSION_CURRENT = VERSION_START;

    private static final GenericFormatMetaInformation META = new GenericFormatMetaInformation(
        VECTOR_FORMAT_INFO_EXTENSION,
        META_CODEC_NAME,
        VERSION_START,
        VERSION_CURRENT
    );

    private static final DirectIOCapableFlatVectorsFormat defaultVectorFormat = new DirectIOCapableLucene99FlatVectorsFormat(
        ES93GenericFlatVectorScorer.INSTANCE
    );
    private static final DirectIOCapableFlatVectorsFormat bitVectorFormat = new DirectIOCapableLucene99FlatVectorsFormat(
        ES93FlatBitVectorScorer.INSTANCE
    ) {
        @Override
        public String getName() {
            return "ES93BitFlatVectorsFormat";
        }
    };
    private static final DirectIOCapableFlatVectorsFormat bfloat16VectorFormat = new ES93BFloat16FlatVectorsFormat(
        ES93GenericFlatVectorScorer.INSTANCE
    );

    private static final Map<String, DirectIOCapableFlatVectorsFormat> supportedFormats = Map.of(
        defaultVectorFormat.getName(),
        defaultVectorFormat,
        bitVectorFormat.getName(),
        bitVectorFormat,
        bfloat16VectorFormat.getName(),
        bfloat16VectorFormat
    );

    private final DirectIOCapableFlatVectorsFormat writeFormat;
    private final boolean useDirectIO;
    private final boolean onDiskMerge;
    private final boolean directIOMergeWrites;

    public ES93GenericFlatVectorsFormat() {
        this(DenseVectorFieldMapper.ElementType.FLOAT, false, false);
    }

    public ES93GenericFlatVectorsFormat(DenseVectorFieldMapper.ElementType elementType, boolean useDirectIO) {
        this(elementType, useDirectIO, false);
    }

    /**
     * @param useDirectIO whether searches read the raw vectors with direct I/O (the field's {@code on_disk_rescore} option)
     * @param onDiskMerge whether merges use direct I/O for the raw vectors (the field's {@code on_disk_merge} option)
     */
    public ES93GenericFlatVectorsFormat(DenseVectorFieldMapper.ElementType elementType, boolean useDirectIO, boolean onDiskMerge) {
        this(elementType, useDirectIO, onDiskMerge, true);
    }

    /**
     * A format whose merges write the raw vectors through the page cache even with {@code on_disk_merge}
     * on. Plain HNSW needs this: right after a merge writes
     * the merged raw vectors, it builds the graph from them by random access, and searches then score
     * against the same file, so the file ends up in the page cache either way. Writing it with direct
     * I/O would only make that read-back cold. Merge reads of the sources still use direct I/O.
     * Searches read through the page cache too: plain HNSW has no {@code on_disk_rescore}.
     */
    static ES93GenericFlatVectorsFormat withBufferedMergeWrites(DenseVectorFieldMapper.ElementType elementType, boolean onDiskMerge) {
        return new ES93GenericFlatVectorsFormat(elementType, false, onDiskMerge, false);
    }

    /**
     * @param directIOMergeWrites whether a merge may write the raw vectors with direct I/O when the field asks
     *                            for it, see {@link #withBufferedMergeWrites}
     */
    private ES93GenericFlatVectorsFormat(
        DenseVectorFieldMapper.ElementType elementType,
        boolean useDirectIO,
        boolean onDiskMerge,
        boolean directIOMergeWrites
    ) {
        super(NAME);
        writeFormat = switch (elementType) {
            case FLOAT, BYTE -> defaultVectorFormat;
            case BIT -> bitVectorFormat;
            case BFLOAT16 -> bfloat16VectorFormat;
        };
        this.useDirectIO = useDirectIO;
        this.onDiskMerge = onDiskMerge;
        this.directIOMergeWrites = directIOMergeWrites;
    }

    @Override
    public FlatVectorsScorer flatVectorsScorer() {
        return writeFormat.flatVectorsScorer();
    }

    @Override
    public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        // this format decides whether a merge writes the raw files with direct I/O (the field's on_disk_merge,
        // unless the format declines the write side); the raw format applies it, see
        // DirectIOCapableFlatVectorsFormat#directIOMergeWriteState. This format's own metadata file and
        // everything else written for the field keep the original context
        return new ES93GenericFlatVectorsWriter(
            META,
            writeFormat.getName(),
            useDirectIO,
            onDiskMerge,
            state,
            writeFormat.fieldsWriter(state, onDiskMerge && directIOMergeWrites)
        );
    }

    @Override
    public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new ES93GenericFlatVectorsReader(META, state, (f, dio, odm) -> {
            var format = supportedFormats.get(f);
            if (format == null) return null;
            return format.fieldsReader(state, dio, odm);
        });
    }

    @Override
    public String toString() {
        return getName() + "(name=" + getName() + ", format=" + writeFormat + ")";
    }
}
