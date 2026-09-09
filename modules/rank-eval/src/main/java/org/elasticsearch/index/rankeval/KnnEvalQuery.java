/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.rankeval;

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
 * One query vector in a {@code _knn_eval} request, together with the id that its results are reported under.
 * <p>
 * An explicit id (rather than the position in the request array) is what lets a caller sweep several candidates against one baseline and
 * still line the per-query numbers up across responses. When the query set is sampled server-side the id is the {@code _id} of the
 * sampled document, which additionally lets the transport action drop the document from its own result list -- see
 * {@link TransportKnnEvalAction}.
 * <p>
 * {@code query_vector} accepts everything a {@code knn} search section accepts: a JSON array of numbers, or a hex or base64 encoded
 * string. Sweeping a few thousand high-dimensional queries is exactly the case the encoded forms exist for, so the vector is carried as
 * {@link VectorData} and handed to the kNN search untouched; an encoded string is only decoded once the field's element type and
 * dimensions are known.
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
        // Only the decoded forms have a length to check here. An encoded string is validated when the mapper decodes it against the
        // field's element type and dimensions, which surfaces as a failure for that one query rather than rejecting the whole sweep.
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
