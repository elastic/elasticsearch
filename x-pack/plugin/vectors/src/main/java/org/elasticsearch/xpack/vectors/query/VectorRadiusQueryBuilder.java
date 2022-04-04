/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.search.KnnVectorQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.vectors.mapper.DenseVectorFieldMapper;
import org.elasticsearch.xpack.vectors.mapper.DenseVectorFieldMapper.DenseVectorFieldType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A query that performs kNN search using Lucene's {@link KnnVectorQuery}.
 *
 * NOTE: this is an internal class and should not be used outside of core Elasticsearch code.
 */
public class VectorRadiusQueryBuilder extends AbstractQueryBuilder<VectorRadiusQueryBuilder> {
    public static final String NAME = "vector_radius";

    private final String fieldName;
    private final float[] origin;
    private final float radius;
    private final int numCands;

    public VectorRadiusQueryBuilder(String fieldName, float[] origin, float radius, int numCands) {
        this.fieldName = fieldName;
        this.origin = origin;
        this.radius = radius;
        this.numCands = numCands;
    }

    public VectorRadiusQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fieldName = in.readString();
        this.numCands = in.readVInt();
        this.origin = in.readFloatArray();
        this.radius = in.readFloat();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeFloatArray(origin);
        out.writeFloat(radius);
        out.writeVInt(numCands);
    }

    private static final ParseField FIELD_FIELD = new ParseField("field");
    private static final ParseField ORIGIN_FIELD = new ParseField("origin");
    private static final ParseField RADIUS_FIELD = new ParseField("radius");
    private static final ParseField NUM_CANDS_FIELD = new ParseField("num_candidates");

    private static final ConstructingObjectParser<VectorRadiusQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(
        "vector_radius",
        args -> {
            @SuppressWarnings("unchecked")
            List<Float> vector = (List<Float>) args[1];
            float[] vectorArray = new float[vector.size()];
            for (int i = 0; i < vector.size(); i++) {
                vectorArray[i] = vector.get(i);
            }
            return new VectorRadiusQueryBuilder((String) args[0], vectorArray, (float) args[2], (int) args[3]);
        }
    );

    static {
        PARSER.declareString(constructorArg(), FIELD_FIELD);
        PARSER.declareFloatArray(constructorArg(), ORIGIN_FIELD);
        PARSER.declareFloat(constructorArg(), RADIUS_FIELD);
        PARSER.declareInt(constructorArg(), NUM_CANDS_FIELD);
        PARSER.declareFloat(VectorRadiusQueryBuilder::boost, BOOST_FIELD);
        PARSER.declareString(VectorRadiusQueryBuilder::queryName, NAME_FIELD);
    }

    public static VectorRadiusQueryBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME)
            .field("field", fieldName)
            .field("origin", origin)
            .field("radius", radius)
            .field("num_candidates", numCands);
        boostAndQueryNameToXContent(builder);
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
            throw new IllegalArgumentException("field [" + fieldName + "] does not exist in the mapping");
        }

        if (fieldType instanceof DenseVectorFieldType == false) {
            throw new IllegalArgumentException(
                "[" + NAME + "] queries are only supported on [" + DenseVectorFieldMapper.CONTENT_TYPE + "] fields"
            );
        }

        DenseVectorFieldType vectorFieldType = (DenseVectorFieldType) fieldType;
        return vectorFieldType.createRadiusQuery(origin, numCands, radius);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, Arrays.hashCode(origin), numCands, radius);
    }

    @Override
    protected boolean doEquals(VectorRadiusQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
            && Arrays.equals(origin, other.origin)
            && numCands == other.numCands
            && radius == other.radius;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_8_3_0;
    }

}
