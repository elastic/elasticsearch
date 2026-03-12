/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es93;

import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
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
        ES93FlatVectorScorer.INSTANCE
    );
    private static final DirectIOCapableFlatVectorsFormat bitVectorFormat = new DirectIOCapableLucene99FlatVectorsFormat(
        ES93FlatBitVectorScorer.INSTANCE
    ) {
        @Override
        public String getName() {
            return "ES93BitFlatVectorsFormat";
        }
    };
    // TODO: a separate scorer for bfloat16
    private static final DirectIOCapableFlatVectorsFormat bfloat16VectorFormat = new ES93BFloat16FlatVectorsFormat(
        FlatVectorScorerUtil.getLucene99FlatVectorsScorer()
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

    public ES93GenericFlatVectorsFormat() {
        this(DenseVectorFieldMapper.ElementType.FLOAT, false);
    }

    public ES93GenericFlatVectorsFormat(DenseVectorFieldMapper.ElementType elementType, boolean useDirectIO) {
        super(NAME);
        writeFormat = switch (elementType) {
            case FLOAT, BYTE -> defaultVectorFormat;
            case BIT -> bitVectorFormat;
            case BFLOAT16 -> bfloat16VectorFormat;
        };
        this.useDirectIO = useDirectIO;
    }

    @Override
    public FlatVectorsScorer flatVectorsScorer() {
        return writeFormat.flatVectorsScorer();
    }

    @Override
    public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new ES93GenericFlatVectorsWriter(META, writeFormat.getName(), useDirectIO, state, writeFormat.fieldsWriter(state));
    }

    @Override
    public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new ES93GenericFlatVectorsReader(META, state, (f, dio) -> {
            var format = supportedFormats.get(f);
            if (format == null) return null;
            return format.fieldsReader(state, dio);
        });
    }

    @Override
    public String toString() {
        return getName() + "(name=" + getName() + ", format=" + writeFormat + ")";
    }
}
