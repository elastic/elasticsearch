/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.vectors.query;


import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.xpack.vectors.mapper.DenseVectorFieldMapper;
import org.elasticsearch.xpack.vectors.mapper.DenseVectorFieldMapper.DenseVectorFieldType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A query that finds approximate nearest neighbours based on product quantization and kmeans clustering
 */
public class AnnPQQueryBuilder extends AbstractQueryBuilder<AnnPQQueryBuilder> {

    public static final String NAME = "ann_pq";
    public static final ParseField FIELD_FIELD = new ParseField("field");
    public static final ParseField QUERY_VECTOR_FIELD = new ParseField("query_vector");


    private static ConstructingObjectParser<AnnPQQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(NAME, false,
        args -> {
            @SuppressWarnings("unchecked") List<Float> qvList = (List<Float>) args[1];
            float[] qv = new float[qvList.size()];
            int i = 0;
            for (Float f : qvList) {
                qv[i++] = f;
            };
            AnnPQQueryBuilder AnnQueryBuilder = new AnnPQQueryBuilder((String) args[0], qv);
            return AnnQueryBuilder;
        });

    static {
        PARSER.declareString(constructorArg(), FIELD_FIELD);
        PARSER.declareFloatArray(constructorArg(), QUERY_VECTOR_FIELD);
    }

    public static AnnPQQueryBuilder fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String field;
    private final float[] queryVector;

    public AnnPQQueryBuilder(String field, float[] queryVector) {
        this.field = field;
        this.queryVector = queryVector;
    }

    public AnnPQQueryBuilder(StreamInput in) throws IOException {
        super(in);
        field = in.readString();
        queryVector = in.readFloatArray();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeFloatArray(queryVector);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(FIELD_FIELD.getPreferredName(), field);
        builder.field(QUERY_VECTOR_FIELD.getPreferredName(), queryVector);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected boolean doEquals(AnnPQQueryBuilder other) {
        return this.field.equals(other.field) &&
            Arrays.equals(this.queryVector, other.queryVector);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(this.field, this.queryVector);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        MappedFieldType fieldType = context.getMapperService().fullName(field);
        if ((fieldType instanceof DenseVectorFieldType) == false ){
            throw new IllegalArgumentException("Field [" + field +
                "] is not of the expected type of [" + DenseVectorFieldMapper.CONTENT_TYPE + "]");
        }
        DenseVectorFieldType dfieldType = (DenseVectorFieldType) fieldType;
        if (dfieldType.ann().equals("pq") == false) {
            throw new IllegalArgumentException("Field [" + field + "] is not indexed with product quantization!");
        }
        return new AnnPQQuery(dfieldType,queryVector);
    }

}
