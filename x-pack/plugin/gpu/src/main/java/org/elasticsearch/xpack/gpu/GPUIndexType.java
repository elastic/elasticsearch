/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MappingParser;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.gpu.codec.GPUVectorsFormat;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

class GPUIndexType implements DenseVectorFieldMapper.VectorIndexType {

    static final String GPU_INDEX_TYPE_NAME = "gpu";

    static final GPUIndexType GPU_INDEX_TYPE = new GPUIndexType();

    @Override
    public String name() {
        return GPU_INDEX_TYPE_NAME;
    }

    @Override
    public DenseVectorFieldMapper.IndexOptions parseIndexOptions(
        String fieldName,
        Map<String, ?> indexOptionsMap,
        IndexVersion indexVersion
    ) {
        MappingParser.checkNoRemainingFields(fieldName, indexOptionsMap);
        return new GPUIndexOptions();
    }

    @Override
    public boolean supportsElementType(DenseVectorFieldMapper.ElementType elementType) {
        return elementType == DenseVectorFieldMapper.ElementType.FLOAT;
    }

    @Override
    public boolean supportsDimension(int dims) {
        return true;
    }

    @Override
    public boolean isQuantized() {
        return false;
    }

    static class GPUIndexOptions extends DenseVectorFieldMapper.IndexOptions {

        GPUIndexOptions() {
            super(GPU_INDEX_TYPE);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("type", type.name());
            builder.endObject();
            return builder;
        }

        @Override
        public KnnVectorsFormat getVectorsFormat(DenseVectorFieldMapper.ElementType elementType) {
            assert elementType == DenseVectorFieldMapper.ElementType.FLOAT;
            return new GPUVectorsFormat();
        }

        @Override
        public boolean updatableTo(DenseVectorFieldMapper.IndexOptions update) {
            return false;
        }

        @Override
        protected boolean doEquals(DenseVectorFieldMapper.IndexOptions o) {
            return o instanceof GPUIndexOptions;
        }

        @Override
        protected int doHashCode() {
            return Objects.hash(type);
        }
    }
}
