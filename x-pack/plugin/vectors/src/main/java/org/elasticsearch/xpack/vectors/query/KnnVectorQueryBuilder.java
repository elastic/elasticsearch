/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.search.KnnVectorQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.vectors.mapper.DenseVectorFieldMapper;
import org.elasticsearch.xpack.vectors.mapper.DenseVectorFieldMapper.DenseVectorFieldType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public class KnnVectorQueryBuilder extends AbstractQueryBuilder<KnnVectorQueryBuilder> {
    public static final String NAME = "knn";

    private final String fieldName;
    private final float[] queryVector;
    private final int numCands;

    public KnnVectorQueryBuilder(String fieldName, float[] queryVector, int numCands) {
        this.fieldName = fieldName;
        this.queryVector = queryVector;
        this.numCands = numCands;
    }

    public KnnVectorQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.numCands = in.readVInt();
        this.queryVector = in.readFloatArray();
    }

    public String getFieldName() {
        return fieldName;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeVInt(numCands);
        out.writeFloatArray(queryVector);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME)
            .field("field", fieldName)
            .field("vector", queryVector)
            .field("num_cands", numCands);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) {
        MappedFieldType fieldType = context.getFieldType(fieldName);
        if (fieldType == null) {
            throw new IllegalStateException("Rewrite first");
        }

        if (fieldType instanceof DenseVectorFieldType == false) {
            throw new IllegalArgumentException("[" + NAME + "] queries are only supported on ["
                + DenseVectorFieldMapper.CONTENT_TYPE + "] fields");
        }

        DenseVectorFieldType vectorFieldType = (DenseVectorFieldType) fieldType;
        if (queryVector.length != vectorFieldType.dims()) {
            throw new IllegalArgumentException("the query vector has a different dimension [" + queryVector.length + "] "
                + "than the index vectors [" + vectorFieldType.dims() + "]");
        }
        if (vectorFieldType.isSearchable() == false) {
            throw new IllegalArgumentException("[" + "[" + NAME + "] queries are not supported if [index] is disabled");
        }
        return new KnnVectorQuery(fieldName, queryVector, numCands);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, Arrays.hashCode(queryVector));
    }

    @Override
    protected boolean doEquals(KnnVectorQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName) &&
            Arrays.equals(queryVector, other.queryVector);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) {
        SearchExecutionContext context = queryRewriteContext.convertToSearchExecutionContext();
        if (context != null && context.getFieldType(fieldName) == null) {
            return new MatchNoneQueryBuilder();
        }
        return this;
    }
}
