/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.IndexOptions;
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper;
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
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        SemanticTextIndexOptions otherSemanticTextIndexOptions = (SemanticTextIndexOptions) other;
        return type == otherSemanticTextIndexOptions.type && Objects.equals(indexOptions, otherSemanticTextIndexOptions.indexOptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, indexOptions);
    }

    public enum SupportedIndexOptions {
        DENSE_VECTOR("dense_vector") {
            @Override
            public IndexOptions parseIndexOptions(
                String fieldName,
                Map<String, Object> map,
                IndexVersion indexVersion,
                boolean experimentalFeaturesEnabled
            ) {
                return parseDenseVectorIndexOptionsFromMap(fieldName, map, indexVersion, experimentalFeaturesEnabled);
            }
        },
        SPARSE_VECTOR("sparse_vector") {
            @Override
            public IndexOptions parseIndexOptions(
                String fieldName,
                Map<String, Object> map,
                IndexVersion indexVersion,
                boolean experimentalFeaturesEnabled
            ) {
                return parseSparseVectorIndexOptionsFromMap(map);
            }
        };

        public final String value;

        SupportedIndexOptions(String value) {
            this.value = value;
        }

        public abstract IndexOptions parseIndexOptions(
            String fieldName,
            Map<String, Object> map,
            IndexVersion indexVersion,
            boolean experimentalFeaturesEnabled
        );

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

    private static ExtendedDenseVectorIndexOptions parseDenseVectorIndexOptionsFromMap(
        String fieldName,
        Map<String, Object> map,
        IndexVersion indexVersion,
        boolean experimentalFeaturesEnabled
    ) {
        DenseVectorFieldMapper.ElementType elementType = null;
        String elementTypeStr = XContentMapValues.nodeStringValue(
            map.remove(ExtendedDenseVectorIndexOptions.ELEMENT_TYPE_FIELD.getPreferredName())
        );
        if (elementTypeStr != null) {
            elementType = DenseVectorFieldMapper.namesToElementType.get(elementTypeStr);
            if (elementType == null) {
                throw new IllegalArgumentException(
                    "Invalid "
                        + ExtendedDenseVectorIndexOptions.ELEMENT_TYPE_FIELD
                        + " ["
                        + elementTypeStr
                        + "]; available types are "
                        + DenseVectorFieldMapper.namesToElementType.keySet()
                );
            }
        }

        DenseVectorFieldMapper.DenseVectorIndexOptions denseVectorIndexOptions = parseBaseDenseVectorIndexOptionsFromMap(
            fieldName,
            map,
            indexVersion,
            experimentalFeaturesEnabled
        );

        if (elementType == null && denseVectorIndexOptions == null) {
            throw new IllegalArgumentException(
                "Must specify at least [" + ExtendedDenseVectorIndexOptions.ELEMENT_TYPE_FIELD + "] or [" + TYPE_FIELD + "]"
            );
        }

        return new ExtendedDenseVectorIndexOptions(denseVectorIndexOptions, elementType);
    }

    private static DenseVectorFieldMapper.DenseVectorIndexOptions parseBaseDenseVectorIndexOptionsFromMap(
        String fieldName,
        Map<String, Object> map,
        IndexVersion indexVersion,
        boolean experimentalFeaturesEnabled
    ) {
        Object type = map.remove(TYPE_FIELD);
        if (type == null) {
            if (map.isEmpty() == false) {
                throw new IllegalArgumentException(
                    "["
                        + TYPE_FIELD
                        + "] is required when specifying more params than ["
                        + ExtendedDenseVectorIndexOptions.ELEMENT_TYPE_FIELD
                        + "]"
                );
            }

            return null;
        }

        DenseVectorFieldMapper.VectorIndexType vectorIndexType = DenseVectorFieldMapper.VectorIndexType.fromString(
            XContentMapValues.nodeStringValue(type)
        ).orElseThrow(() -> new IllegalArgumentException("Unsupported index options " + TYPE_FIELD + " " + type));

        return vectorIndexType.parseIndexOptions(fieldName, map, indexVersion, experimentalFeaturesEnabled);
    }

    private static SparseVectorFieldMapper.SparseVectorIndexOptions parseSparseVectorIndexOptionsFromMap(Map<String, Object> map) {
        return SparseVectorFieldMapper.SparseVectorIndexOptions.parseFromMap(map);
    }
}
