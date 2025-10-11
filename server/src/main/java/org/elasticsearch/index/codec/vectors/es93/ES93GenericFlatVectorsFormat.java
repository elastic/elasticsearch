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
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.index.codec.vectors.AbstractFlatVectorsFormat;
import org.elasticsearch.index.codec.vectors.DirectIOCapableFlatVectorsFormat;

import java.io.IOException;
import java.util.Map;

public abstract class ES93GenericFlatVectorsFormat extends AbstractFlatVectorsFormat {

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

    public ES93GenericFlatVectorsFormat(String name) {
        super(name);
    }

    protected abstract DirectIOCapableFlatVectorsFormat writeFlatVectorsFormat();

    protected abstract boolean useDirectIOReads();

    protected abstract Map<String, DirectIOCapableFlatVectorsFormat> supportedReadFlatVectorsFormats();

    @Override
    public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        var flatFormat = writeFlatVectorsFormat();
        boolean directIO = useDirectIOReads();
        return new ES93GenericFlatVectorsWriter(META, flatFormat.getName(), directIO, state, flatFormat.fieldsWriter(state));
    }

    @Override
    public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        var readFormats = supportedReadFlatVectorsFormats();
        return new ES93GenericFlatVectorsReader(META, state, (f, dio) -> {
            var format = readFormats.get(f);
            if (format == null) return null;
            return format.fieldsReader(state, dio);
        });
    }

    @Override
    public String toString() {
        return getName()
            + "(name="
            + getName()
            + ", writeFlatVectorFormat="
            + writeFlatVectorsFormat()
            + ", readFlatVectorsFormats="
            + supportedReadFlatVectorsFormats().values()
            + ")";
    }
}
