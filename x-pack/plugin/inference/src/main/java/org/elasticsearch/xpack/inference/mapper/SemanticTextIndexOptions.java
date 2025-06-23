/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.IndexOptions;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Represents index options for a semantic_text field.
 * We represent semantic_text index_options as nested within their respective type. For example:
 * "index_options": {
 *   "dense_vector": {
 *    "type": "bbq_hnsw
 *    }
 *  }
 */
public class SemanticTextIndexOptions implements ToXContent {

    private static final String TYPE_FIELD = "type";

    private final SupportedIndexOptions type;
    private final IndexOptions indexOptions;

    public SemanticTextIndexOptions(SupportedIndexOptions type, IndexOptions indexOptions) {
        this.type = type;
        this.indexOptions = indexOptions;
    }

    public SupportedIndexOptions type() {
        return type;
    }

    public IndexOptions indexOptions() {
        return indexOptions;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof SemanticTextIndexOptions == false) return false;
        SemanticTextIndexOptions that = (SemanticTextIndexOptions) o;
        return type == that.type && Objects.equals(indexOptions, that.indexOptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, indexOptions);
    }

    public enum SupportedIndexOptions {
        DENSE_VECTOR("dense_vector") {
            @Override
            public IndexOptions parseIndexOptions(String fieldName, Map<String, Object> map, IndexVersion indexVersion) {
                return parseDenseVectorIndexOptionsFromMap(fieldName, map, indexVersion);
            }
        };

        public final String value;

        SupportedIndexOptions(String value) {
            this.value = value;
        }

        public abstract IndexOptions parseIndexOptions(String fieldName, Map<String, Object> map, IndexVersion indexVersion);

        public static SupportedIndexOptions fromValue(String value) {
            return Arrays.stream(SupportedIndexOptions.values())
                .filter(option -> option.value.equals(value))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown index options type [" + value + "]"));
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(type.value.toLowerCase(Locale.ROOT));
        indexOptions.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    private static DenseVectorFieldMapper.DenseVectorIndexOptions parseDenseVectorIndexOptionsFromMap(
        String fieldName,
        Map<String, Object> map,
        IndexVersion indexVersion
    ) {
        try {
            Object type = map.remove(TYPE_FIELD);
            if (type == null) {
                throw new IllegalArgumentException("Required " + TYPE_FIELD);
            }
            DenseVectorFieldMapper.VectorIndexType vectorIndexType = DenseVectorFieldMapper.VectorIndexType.fromString(
                XContentMapValues.nodeStringValue(type, null)
            ).orElseThrow(() -> new IllegalArgumentException("Unsupported index options " + TYPE_FIELD + " " + type));

            return vectorIndexType.parseIndexOptions(fieldName, map, indexVersion);
        } catch (Exception exc) {
            throw new ElasticsearchException(exc);
        }
    }
}
