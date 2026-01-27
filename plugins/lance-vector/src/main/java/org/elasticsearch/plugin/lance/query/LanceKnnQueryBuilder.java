/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.lance.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryValidationException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugin.lance.mapper.LanceVectorFieldMapper.LanceVectorFieldType;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Query builder for Lance kNN search.
 * <p>
 * This query bypasses the standard kNN query validation in KnnVectorQueryBuilder
 * by directly calling LanceVectorFieldType.createKnnQuery(). This allows Lance
 * vectors to be searched using custom query syntax:
 * <pre>
 * {
 *   "query": {
 *     "lance_knn": {
 *       "field": "vector",
 *       "query_vector": [0.1, 0.2, ...],
 *       "k": 10,
 *       "num_candidates": 100
 *     }
 *   }
 * }
 * </pre>
 */
public class LanceKnnQueryBuilder extends AbstractQueryBuilder<LanceKnnQueryBuilder> {
    public static final String NAME = "lance_knn";

    private static final ParseField FIELD_FIELD = new ParseField("field");
    private static final ParseField QUERY_VECTOR_FIELD = new ParseField("query_vector");
    private static final ParseField K_FIELD = new ParseField("k");
    private static final ParseField NUM_CANDIDATES_FIELD = new ParseField("num_candidates");

    private final String fieldName;
    private final float[] queryVector;
    private final int k;
    private final int numCandidates;

    /**
     * Construct a new LanceKnnQueryBuilder.
     *
     * @param fieldName     The name of the lance_vector field
     * @param queryVector   The query vector
     * @param k             The number of nearest neighbors to return
     * @param numCandidates The number of candidates to consider (optional)
     */
    public LanceKnnQueryBuilder(String fieldName, float[] queryVector, int k, int numCandidates) {
        this.fieldName = fieldName;
        this.queryVector = queryVector;
        this.k = k;
        this.numCandidates = numCandidates;
    }

    /**
     * Read from stream.
     */
    public LanceKnnQueryBuilder(StreamInput in) throws IOException {
        this.fieldName = in.readString();
        this.queryVector = in.readFloatArray();
        this.k = in.readVInt();
        this.numCandidates = in.readVInt();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeFloatArray(queryVector);
        out.writeVInt(k);
        out.writeVInt(numCandidates);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(FIELD_FIELD.getPreferredName(), fieldName);
        builder.field(QUERY_VECTOR_FIELD.getPreferredName(), queryVector);
        builder.field(K_FIELD.getPreferredName(), k);
        builder.field(NUM_CANDIDATES_FIELD.getPreferredName(), numCandidates);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        MappedFieldType fieldType = context.getFieldType(fieldName);
        if (fieldType instanceof LanceVectorFieldType == false) {
            throw new IllegalArgumentException(
                "field [" + fieldName + "] is not a lance_vector field"
            );
        }

        LanceVectorFieldType lanceFieldType = (LanceVectorFieldType) fieldType;
        VectorData vectorData = VectorData.fromFloats(queryVector);

        // Call Lance's createKnnQuery with full signature matching DenseVectorFieldType
        // Lance ignores most of these parameters since it uses external storage
        return lanceFieldType.createKnnQuery(
            vectorData,
            k,
            numCandidates,
            null,  // visitPercentage - ignored by Lance
            null,  // oversample - ignored by Lance
            null,  // filter - TODO: support filters in lance_knn query
            null,  // vectorSimilarity - ignored by Lance
            null,  // parentFilter - ignored by Lance (no nested support)
            null,  // heuristic - ignored by Lance
            false  // hnswEarlyTermination - ignored by Lance
        );
    }

    protected QueryValidationException validate(SearchExecutionContext context) {
        QueryValidationException validationException = null;
        if (fieldName == null || fieldName.isEmpty()) {
            validationException = QueryValidationException.addValidationError(
                "lance_knn",
                "field name is null or empty",
                validationException
            );
        }
        if (queryVector == null || queryVector.length == 0) {
            validationException = QueryValidationException.addValidationError(
                "lance_knn",
                "query_vector is null or empty",
                validationException
            );
        }
        if (k <= 0) {
            validationException = QueryValidationException.addValidationError(
                "lance_knn",
                "k must be positive, got [" + k + "]",
                validationException
            );
        }
        if (numCandidates <= 0) {
            validationException = QueryValidationException.addValidationError(
                "lance_knn",
                "num_candidates must be positive, got [" + numCandidates + "]",
                validationException
            );
        }
        return validationException;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, Arrays.hashCode(queryVector), k, numCandidates);
    }

    @Override
    protected boolean doEquals(LanceKnnQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
            && Arrays.equals(queryVector, other.queryVector)
            && Objects.equals(k, other.k)
            && Objects.equals(numCandidates, other.numCandidates);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.zero();
    }

    /**
     * Parse a LanceKnnQueryBuilder from XContent.
     */
    public static LanceKnnQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String fieldName = null;
        float[] queryVector = null;
        int k = 10;
        int numCandidates = 100;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (FIELD_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    throw new IllegalArgumentException("field [" + currentFieldName + "] should be a string");
                } else if (QUERY_VECTOR_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryVector = parseQueryVector(parser);
                } else {
                    throw new IllegalArgumentException("unknown field [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (FIELD_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    fieldName = parser.text();
                } else if (K_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    k = parser.intValue(true);
                } else if (NUM_CANDIDATES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    numCandidates = parser.intValue(true);
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else {
                    throw new IllegalArgumentException("unknown field [" + currentFieldName + "]");
                }
            }
        }

        if (fieldName == null) {
            throw new IllegalArgumentException("field [" + FIELD_FIELD.getPreferredName() + "] is required");
        }
        if (queryVector == null) {
            throw new IllegalArgumentException("query_vector is required");
        }

        LanceKnnQueryBuilder builder = new LanceKnnQueryBuilder(fieldName, queryVector, k, numCandidates);
        builder.boost(boost);
        if (queryName != null) {
            builder.queryName(queryName);
        }
        return builder;
    }

    /**
     * Parse a query vector from XContent.
     */
    private static float[] parseQueryVector(XContentParser parser) throws IOException {
        java.util.ArrayList<Float> vector = new java.util.ArrayList<>();
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            vector.add((float) parser.doubleValue());
        }
        float[] result = new float[vector.size()];
        for (int i = 0; i < vector.size(); i++) {
            result[i] = vector.get(i);
        }
        return result;
    }
}
