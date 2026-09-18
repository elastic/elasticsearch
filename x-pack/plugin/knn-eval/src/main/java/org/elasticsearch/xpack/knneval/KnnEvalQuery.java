/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.knneval;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Associates a stable identifier with a query vector. Sampled identifiers also identify self-hits that must be excluded.
 */
record KnnEvalQuery(String id, VectorData queryVector) implements Writeable, ToXContentObject {

    static final ParseField ID_FIELD = new ParseField("id");
    static final ParseField QUERY_VECTOR_FIELD = new ParseField("query_vector");

    private static final ConstructingObjectParser<KnnEvalQuery, Void> PARSER = new ConstructingObjectParser<>(
        "knn_eval_query",
        args -> new KnnEvalQuery((String) args[0], (VectorData) args[1])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ID_FIELD);
        PARSER.declareField(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> VectorData.parseXContent(p),
            QUERY_VECTOR_FIELD,
            ObjectParser.ValueType.OBJECT_ARRAY_STRING_OR_NUMBER
        );
    }

    /** Creates a query with a non-empty identifier and vector. */
    KnnEvalQuery {
        id = Objects.requireNonNull(id, "query id must not be null");
        queryVector = Objects.requireNonNull(queryVector, "query vector must not be null");
        if (Strings.hasText(id) == false) {
            throw new IllegalArgumentException("[" + ID_FIELD.getPreferredName() + "] must not be empty");
        }
        // an encoded string has no length to check here; the mapper's decode failure surfaces against that one query
        if (queryVector.isStringVector() == false && queryVector.size() == 0) {
            throw new IllegalArgumentException("[" + QUERY_VECTOR_FIELD.getPreferredName() + "] must not be empty for query [" + id + "]");
        }
    }

    KnnEvalQuery(StreamInput in) throws IOException {
        this(in.readString(), new VectorData(in));
    }

    static KnnEvalQuery fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public String getId() {
        return id;
    }

    public VectorData getQueryVector() {
        return queryVector;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        queryVector.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID_FIELD.getPreferredName(), id);
        builder.field(QUERY_VECTOR_FIELD.getPreferredName(), queryVector);
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

}
