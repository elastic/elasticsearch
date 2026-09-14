/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.knneval;

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
 * One query vector and the id its results are reported under. An explicit id, rather than an array position, lets per-query numbers be
 * lined up across the responses of a sweep; for a sampled query it is the source document's {@code _id}, which is also how
 * {@link TransportKnnEvalAction} drops that document from the results.
 * <p>
 * The vector is carried as {@link VectorData} and handed to the kNN search untouched, so an encoded string is decoded only once the
 * field's element type and dimensions are known.
 */
public class KnnEvalQuery implements Writeable, ToXContentObject {

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

    private final String id;
    private final VectorData queryVector;

    public KnnEvalQuery(String id, VectorData queryVector) {
        this.id = Objects.requireNonNull(id, "query id must not be null");
        this.queryVector = Objects.requireNonNull(queryVector, "query vector must not be null");
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

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        KnnEvalQuery other = (KnnEvalQuery) obj;
        return Objects.equals(id, other.id) && Objects.equals(queryVector, other.queryVector);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, queryVector);
    }
}
