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

import java.io.IOException;
import java.util.Map;

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

    private static final FlatVectorsScorer scorer = FlatVectorScorerUtil.getLucene99FlatVectorsScorer();

    private static final DirectIOCapableFlatVectorsFormat float32VectorFormat = new DirectIOCapableLucene99FlatVectorsFormat(scorer);
    // TODO: a separate scorer for bfloat16
    private static final DirectIOCapableFlatVectorsFormat bfloat16VectorFormat = new ES93BFloat16FlatVectorsFormat(scorer);

    private static final Map<String, DirectIOCapableFlatVectorsFormat> supportedFormats = Map.of(
        float32VectorFormat.getName(),
        float32VectorFormat,
        bfloat16VectorFormat.getName(),
        bfloat16VectorFormat
    );

    private final DirectIOCapableFlatVectorsFormat writeFormat;
    private final boolean useDirectIO;

    public ES93GenericFlatVectorsFormat() {
        this(false, false);
    }

    public ES93GenericFlatVectorsFormat(boolean useBFloat16, boolean useDirectIO) {
        super(NAME);
        writeFormat = useBFloat16 ? bfloat16VectorFormat : float32VectorFormat;
        this.useDirectIO = useDirectIO;
    }

    @Override
    protected FlatVectorsScorer flatVectorsScorer() {
        return scorer;
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
