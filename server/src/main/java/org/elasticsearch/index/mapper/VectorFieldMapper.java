/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90HnswVectorsFormat;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.apache.lucene.codecs.lucene90.Lucene90HnswVectorsFormat.DEFAULT_MAX_CONN;
import static org.apache.lucene.codecs.lucene90.Lucene90HnswVectorsFormat.DEFAULT_BEAM_WIDTH;

/**
 * Field mapper for a vector field for ann search.
 */

public abstract class VectorFieldMapper extends FieldMapper {
    public static final IndexOptions DEFAULT_INDEX_OPTIONS = new HNSWIndexOptions(DEFAULT_MAX_CONN, DEFAULT_BEAM_WIDTH);
    protected final IndexOptions indexOptions;

    protected VectorFieldMapper(String simpleName, MappedFieldType mappedFieldType, MultiFields multiFields, CopyTo copyTo,
            IndexOptions indexOptions) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.indexOptions = indexOptions;
    }

    /**
     * Returns the knn vectors format that is customly set up for this field or {@code null} if
     * the format is not set up or if the set up format matches the default format.
     * @return the knn vectors format for the field, or {@code null} if the default format should be used
     */
    public KnnVectorsFormat getKnnVectorsFormatForField() {
        if (indexOptions == null && indexOptions == DEFAULT_INDEX_OPTIONS) {
            return null;
        } else {
            HNSWIndexOptions hnswIndexOptions = (HNSWIndexOptions) indexOptions;
            return new Lucene90HnswVectorsFormat(hnswIndexOptions.m, hnswIndexOptions.efConstruction);
        }
    }

    public static IndexOptions parseVectorIndexOptions(String fieldName, Object propNode) {
        if (propNode == null) {
            return null;
        }
        Map<?, ?> indexOptionsMap = (Map<?, ?>) propNode;
        String type = XContentMapValues.nodeStringValue(indexOptionsMap.remove("type"), "hnsw");
        if (type.equals("hnsw")) {
            return HNSWIndexOptions.parseIndexOptions(fieldName, indexOptionsMap);
        } else {
            throw new MapperParsingException("Unknown vector index options type [" + type + "] for field [" + fieldName + "]");
        }
    }

    public abstract static class IndexOptions implements ToXContent {
        protected final String type;
        public IndexOptions(String type) {
            this.type = type;
        }
    }

    public static class HNSWIndexOptions extends IndexOptions {
        private final int m;
        private final int efConstruction;

        public HNSWIndexOptions(int m, int efConstruction) {
            super("hnsw");
            this.m = m;
            this.efConstruction = efConstruction;
        }

        public int m() {
            return m;
        }

        public int efConstruction() {
            return efConstruction;
        }

        public static IndexOptions parseIndexOptions(String fieldName, Map<?, ?> indexOptionsMap) {
            int m = XContentMapValues.nodeIntegerValue(indexOptionsMap.remove("m"), DEFAULT_MAX_CONN);
            int efConstruction = XContentMapValues.nodeIntegerValue(indexOptionsMap.remove("ef_construction"), DEFAULT_BEAM_WIDTH);
            MappingParser.checkNoRemainingFields(fieldName, indexOptionsMap);
            if (m == DEFAULT_MAX_CONN && efConstruction == DEFAULT_BEAM_WIDTH) {
                return VectorFieldMapper.DEFAULT_INDEX_OPTIONS;
            } else {
                return new HNSWIndexOptions(m, efConstruction);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("type", type);
            builder.field("m", m);
            builder.field("ef_construction", efConstruction);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            HNSWIndexOptions that = (HNSWIndexOptions) o;
            return m == that.m && efConstruction == that.efConstruction;
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, m, efConstruction);
        }

        @Override
        public String toString() {
            return "{type=" + type + ", m=" + m + ", ef_construction=" + efConstruction + " }";
        }
    }
}
