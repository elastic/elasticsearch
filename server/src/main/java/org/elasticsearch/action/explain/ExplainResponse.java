/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.explain;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.HppcMaps;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.rest.action.search.RestExplainAction;
import org.omg.CORBA.MARSHAL;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.lucene.Lucene.readExplanation;
import static org.elasticsearch.common.lucene.Lucene.writeExplanation;

/**
 * Response containing the score explanation.
 */
public class ExplainResponse extends ActionResponse implements ToXContentObject {

    private static final ParseField _INDEX = new ParseField("_index");
    private static final ParseField _TYPE = new ParseField("_type");
    private static final ParseField _ID = new ParseField("_id");
    private static final ParseField MATCHED = new ParseField("matched");
    private static final ParseField EXPLANATION = new ParseField("explanation");
    private static final ParseField VALUE = new ParseField("value");
    private static final ParseField DESCRIPTION = new ParseField("description");
    private static final ParseField DETAILS = new ParseField("details");
    private static final ParseField GET = new ParseField("get");

    private String index;
    private String type;
    private String id;
    private boolean exists;
    private Explanation explanation;
    private GetResult getResult;

    ExplainResponse() {
    }

    public ExplainResponse(String index, String type, String id, boolean exists) {
        this.index = index;
        this.type = type;
        this.id = id;
        this.exists = exists;
    }

    public ExplainResponse(String index, String type, String id, boolean exists, Explanation explanation) {
        this(index, type, id, exists);
        this.explanation = explanation;
    }

    public ExplainResponse(String index, String type, String id, boolean exists, Explanation explanation, GetResult getResult) {
        this(index, type, id, exists, explanation);
        this.getResult = getResult;
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
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

    public void setExists(boolean exists) {
        this.exists = exists;
    }

    public boolean isExists() {
        return exists;
    }

    public GetResult getGetResult() {
        return getResult;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        index = in.readString();
        type = in.readString();
        id = in.readString();
        exists = in.readBoolean();
        if (in.readBoolean()) {
            explanation = readExplanation(in);
        }
        if (in.readBoolean()) {
            getResult = GetResult.readGetResult(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeString(type);
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

    private static final ConstructingObjectParser<ExplainResponse, Void> PARSER = new ConstructingObjectParser<>("explain", true,
        arg -> new ExplainResponse((String) arg[0], (String) arg[1], (String) arg[2], (boolean) arg[3], (Explanation) arg[4],
            (GetResult) arg[5]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), _INDEX);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), _TYPE);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), _ID);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), MATCHED);
        final ConstructingObjectParser<Explanation, Void> explanationParser = new ConstructingObjectParser<>("explanation", true,
            arg -> {
                if ((float) arg[0] > 0) {
                    return Explanation.match((float) arg[0], (String) arg[1], (Collection<Explanation>) arg[2]);
                } else {
                    return Explanation.noMatch((String) arg[1], (Collection<Explanation>) arg[2]);
                }
            });
        explanationParser.declareFloat(ConstructingObjectParser.constructorArg(), VALUE);
        explanationParser.declareString(ConstructingObjectParser.constructorArg(), DESCRIPTION);
        explanationParser.declareObjectArray(ConstructingObjectParser.constructorArg(), explanationParser, DETAILS);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), explanationParser, EXPLANATION);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> GetResult.fromXContentEmbedded(p), GET);
    }

    public static ExplainResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(_INDEX.getPreferredName(), index);
        builder.field(_TYPE.getPreferredName(), type);
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

    private void buildExplanation(XContentBuilder builder, Explanation explanation) throws IOException {
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
            && type.equals(other.type)
            && id.equals(other.id)
            && exists == other.exists
            && Objects.equals(explanation, other.explanation)
            && getResult.isExists() == other.getResult.isExists()
            && Objects.equals(getResult.sourceAsMap(), other.getResult.sourceAsMap())
            && Objects.equals(getResult.getFields(), other.getResult.getFields());
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, type, id, exists, explanation, getResult.isExists(), getResult.sourceAsMap(), getResult.getFields());
    }
}
