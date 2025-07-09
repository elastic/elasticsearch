/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.explain;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.lucene.Lucene.readExplanation;
import static org.elasticsearch.common.lucene.Lucene.writeExplanation;

/**
 * Response containing the score explanation.
 */
public class ExplainResponse extends ActionResponse implements ToXContentObject {

    static final ParseField _INDEX = new ParseField("_index");
    static final ParseField _ID = new ParseField("_id");
    private static final ParseField MATCHED = new ParseField("matched");
    static final ParseField EXPLANATION = new ParseField("explanation");
    static final ParseField VALUE = new ParseField("value");
    static final ParseField DESCRIPTION = new ParseField("description");
    static final ParseField DETAILS = new ParseField("details");
    static final ParseField GET = new ParseField("get");

    private final String index;
    private final String id;
    private final boolean exists;
    private Explanation explanation;
    private GetResult getResult;

    public ExplainResponse(String index, String id, boolean exists) {
        this.index = index;
        this.id = id;
        this.exists = exists;
    }

    public ExplainResponse(String index, String id, boolean exists, Explanation explanation) {
        this(index, id, exists);
        this.explanation = explanation;
    }

    public ExplainResponse(String index, String id, boolean exists, Explanation explanation, GetResult getResult) {
        this(index, id, exists, explanation);
        this.getResult = getResult;
    }

    public ExplainResponse(StreamInput in) throws IOException {
        index = in.readString();
        if (in.getTransportVersion().before(TransportVersions.V_8_0_0)) {
            in.readString();
        }
        id = in.readString();
        exists = in.readBoolean();
        if (in.readBoolean()) {
            explanation = readExplanation(in);
        }
        if (in.readBoolean()) {
            getResult = new GetResult(in);
        }
    }

    public String getIndex() {
        return index;
    }

    public String getId() {
        return id;
    }

    public Explanation getExplanation() {
        return explanation;
    }

    public boolean isMatch() {
        return explanation != null && explanation.isMatch();
    }

    public boolean hasExplanation() {
        return explanation != null;
    }

    public boolean isExists() {
        return exists;
    }

    public GetResult getGetResult() {
        return getResult;
    }

    public RestStatus status() {
        return exists ? RestStatus.OK : RestStatus.NOT_FOUND;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        if (out.getTransportVersion().before(TransportVersions.V_8_0_0)) {
            out.writeString(MapperService.SINGLE_MAPPING_NAME);
        }
        out.writeString(id);
        out.writeBoolean(exists);
        if (explanation == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            writeExplanation(out, explanation);
        }
        if (getResult == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            getResult.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(_INDEX.getPreferredName(), index);
        builder.field(_ID.getPreferredName(), id);
        builder.field(MATCHED.getPreferredName(), isMatch());
        if (hasExplanation()) {
            builder.startObject(EXPLANATION.getPreferredName());
            buildExplanation(builder, explanation);
            builder.endObject();
        }
        if (getResult != null) {
            builder.startObject(GET.getPreferredName());
            getResult.toXContentEmbedded(builder, params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    private static void buildExplanation(XContentBuilder builder, Explanation explanation) throws IOException {
        builder.field(VALUE.getPreferredName(), explanation.getValue());
        builder.field(DESCRIPTION.getPreferredName(), explanation.getDescription());
        Explanation[] innerExps = explanation.getDetails();
        if (innerExps != null) {
            builder.startArray(DETAILS.getPreferredName());
            for (Explanation exp : innerExps) {
                builder.startObject();
                buildExplanation(builder, exp);
                builder.endObject();
            }
            builder.endArray();
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ExplainResponse other = (ExplainResponse) obj;
        return index.equals(other.index)
            && id.equals(other.id)
            && Objects.equals(explanation, other.explanation)
            && getResult.isExists() == other.getResult.isExists()
            && Objects.equals(getResult.sourceAsMap(), other.getResult.sourceAsMap())
            && Objects.equals(getResult.getFields(), other.getResult.getFields());
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, id, explanation, getResult.isExists(), getResult.sourceAsMap(), getResult.getFields());
    }
}
