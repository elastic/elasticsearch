/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.lance.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MappingParserContext;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugin.lance.query.LanceKnnQuery;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class LanceVectorFieldMapper extends FieldMapper {
    public static final String CONTENT_TYPE = "lance_vector";
    private static final String STORAGE_FIELD = "storage";
    private static final String DIMS_FIELD = "dims";
    private static final String SIMILARITY_FIELD = "similarity";

    public static class Builder extends FieldMapper.Builder {
        private final int dims;
        private final LanceStorageConfig storage;
        private final String similarity;
        private final IndexVersion indexVersionCreated;

        public Builder(String name, int dims, String similarity, LanceStorageConfig storage, IndexVersion indexVersionCreated) {
            super(name);
            this.dims = dims;
            this.similarity = similarity == null ? "cosine" : similarity;
            this.storage = storage;
            this.indexVersionCreated = indexVersionCreated;
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[0];
        }

        @Override
        public LanceVectorFieldMapper build(MapperBuilderContext context) {
            LanceVectorFieldType fieldType = new LanceVectorFieldType(context.buildFullName(leafName()), dims, similarity, storage);
            return new LanceVectorFieldMapper(leafName(), fieldType, builderParams(this, context));
        }
    }

    public static final Mapper.TypeParser PARSER = new Mapper.TypeParser() {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, MappingParserContext parserContext)
            throws MapperParsingException {
            Object dimsObj = node.remove(DIMS_FIELD);
            if (dimsObj == null) {
                throw new MapperParsingException("[dims] is required for lance_vector");
            }
            int dims;
            if (dimsObj instanceof Number) {
                dims = ((Number) dimsObj).intValue();
            } else {
                throw new MapperParsingException("[dims] must be an integer for lance_vector");
            }

            String similarity = "cosine";
            Object simObj = node.remove(SIMILARITY_FIELD);
            if (simObj != null) {
                similarity = simObj.toString();
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> storage = (Map<String, Object>) node.remove(STORAGE_FIELD);
            if (storage == null) {
                storage = Map.of();
            }

            String type = "external";
            Object typeObj = storage.get("type");
            if (typeObj != null) {
                type = typeObj.toString();
            }

            Object uriObj = storage.get("uri");
            if (uriObj == null) {
                throw new MapperParsingException("[storage.uri] is required for lance_vector");
            }
            String uri = uriObj.toString();

            String idColumn = "_id";
            Object idColObj = storage.get("lance_id_column");
            if (idColObj != null) {
                idColumn = idColObj.toString();
            }

            String vectorColumn = "vector";
            Object vecColObj = storage.get("lance_vector_column");
            if (vecColObj != null) {
                vectorColumn = vecColObj.toString();
            }

            // OSS configuration (optional)
            String ossEndpoint = null;
            Object ossEndpointObj = storage.get("oss_endpoint");
            if (ossEndpointObj != null) {
                ossEndpoint = ossEndpointObj.toString();
            }

            String ossAccessKeyId = null;
            Object ossAccessKeyIdObj = storage.get("oss_access_key_id");
            if (ossAccessKeyIdObj != null) {
                ossAccessKeyId = ossAccessKeyIdObj.toString();
            }

            String ossAccessKeySecret = null;
            Object ossAccessKeySecretObj = storage.get("oss_access_key_secret");
            if (ossAccessKeySecretObj != null) {
                ossAccessKeySecret = ossAccessKeySecretObj.toString();
            }

            boolean readOnly = true;
            Object readOnlyObj = storage.get("read_only");
            if (readOnlyObj != null) {
                readOnly = Booleans.parseBoolean(readOnlyObj.toString());
            }

            if (readOnly == false) {
                throw new MapperParsingException("Phase 1 lance_vector supports only read_only external datasets");
            }

            LanceStorageConfig storageConfig = new LanceStorageConfig(type, uri, idColumn, vectorColumn,
                ossEndpoint, ossAccessKeyId, ossAccessKeySecret);
            return new Builder(name, dims, similarity, storageConfig, parserContext.getIndexSettings().getIndexVersionCreated());
        }
    };

    public static class LanceVectorFieldType extends MappedFieldType {
        private final int dims;
        private final String similarity;
        private final LanceStorageConfig storage;

        public LanceVectorFieldType(String name, int dims, String similarity, LanceStorageConfig storage) {
            super(name, false, false, false, Collections.emptyMap());
            this.dims = dims;
            this.similarity = similarity == null ? "cosine" : similarity;
            this.storage = storage;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            throw new IllegalArgumentException("term queries not supported for lance_vector");
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return SourceValueFetcher.identity(name(), context, format);
        }

        /**
         * Create a kNN query for Lance vector search.
         * This method is now compatible with the full DenseVectorFieldType.createKnnQuery signature.
         * Most parameters are ignored because Lance uses external vector storage.
         */
        public Query createKnnQuery(
            VectorData queryVector,
            int k,
            int numCands,
            Float visitPercentage,  // Ignored - Lance doesn't use this
            Float oversample,  // Ignored - Lance doesn't use this
            Query filter,
            Float vectorSimilarity,  // Ignored - Lance uses its own similarity
            org.apache.lucene.search.join.BitSetProducer parentFilter,  // Ignored - Lance doesn't support nested
            DenseVectorFieldMapper.FilterHeuristic heuristic,  // Ignored - Lance uses its own search strategy
            boolean hnswEarlyTermination  // Ignored - Lance doesn't use HNSW
        ) {
            float[] vector = queryVector.isFloat() ? queryVector.asFloatVector() : toFloat(queryVector.asByteVector());
            if (vector.length != dims) {
                throw new IllegalArgumentException("query vector dims mismatch expected=" + dims + " got=" + vector.length);
            }
            return new LanceKnnQuery(name(), storage.uri(), vector, k, numCands, similarity, filter, dims,
                storage.ossEndpoint(), storage.ossAccessKeyId(), storage.ossAccessKeySecret());
        }

        private static float[] toFloat(byte[] bytes) {
            float[] result = new float[bytes.length];
            for (int i = 0; i < bytes.length; i++) {
                result[i] = bytes[i];
            }
            return result;
        }

        public int getDims() {
            return dims;
        }

        public String getSimilarity() {
            return similarity;
        }

        public LanceStorageConfig getStorage() {
            return storage;
        }
    }

    private LanceVectorFieldMapper(String simpleName, MappedFieldType mappedFieldType, BuilderParams params) {
        super(simpleName, mappedFieldType, params);
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        // Phase 1 is read-only; we ignore any provided value. Field exists only in mapping.
        if (context.parser().currentToken() != null) {
            context.parser().skipChildren();
        }
    }

    @Override
    public LanceVectorFieldType fieldType() {
        return (LanceVectorFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        LanceVectorFieldType ft = fieldType();
        return new Builder(leafName(), ft.dims, ft.similarity, ft.storage, IndexVersion.current());
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, Params params) throws IOException {
        super.doXContentBody(builder, params);
        LanceVectorFieldType ft = fieldType();
        builder.field(DIMS_FIELD, ft.dims);
        builder.field(SIMILARITY_FIELD, ft.similarity);
        builder.startObject(STORAGE_FIELD);
        builder.field("type", ft.storage.type());
        builder.field("uri", ft.storage.uri());
        builder.field("lance_id_column", ft.storage.idColumn());
        builder.field("lance_vector_column", ft.storage.vectorColumn());
        builder.field("read_only", true);
        builder.endObject();
    }
}
