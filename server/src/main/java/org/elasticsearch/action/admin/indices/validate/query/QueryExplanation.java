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

package org.elasticsearch.action.admin.indices.validate.query;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class QueryExplanation implements Writeable, ToXContentFragment {

    public static final String INDEX_FIELD = "index";
    public static final String SHARD_FIELD = "shard";
    public static final String VALID_FIELD = "valid";
    public static final String ERROR_FIELD = "error";
    public static final String EXPLANATION_FIELD = "explanation";

    public static final int RANDOM_SHARD = -1;

    static ConstructingObjectParser<QueryExplanation, Void> PARSER = new ConstructingObjectParser<>(
        "query_explanation",
        true,
        a -> {
            int shard = RANDOM_SHARD;
            if (a[1] != null) {
                shard = (int)a[1];
            }
            return new QueryExplanation(
                (String)a[0],
                shard,
                (boolean)a[2],
                (String)a[3],
                (String)a[4]
            );
        }
    );
    static {
        PARSER.declareString(optionalConstructorArg(), new ParseField(INDEX_FIELD));
        PARSER.declareInt(optionalConstructorArg(), new ParseField(SHARD_FIELD));
        PARSER.declareBoolean(constructorArg(), new ParseField(VALID_FIELD));
        PARSER.declareString(optionalConstructorArg(), new ParseField(EXPLANATION_FIELD));
        PARSER.declareString(optionalConstructorArg(), new ParseField(ERROR_FIELD));
    }

    private String index;

    private int shard = RANDOM_SHARD;

    private boolean valid;

    private String explanation;

    private String error;

    public QueryExplanation(StreamInput in) throws IOException {
        index = in.readOptionalString();
        shard = in.readInt();
        valid = in.readBoolean();
        explanation = in.readOptionalString();
        error = in.readOptionalString();
    }

    public QueryExplanation(String index, int shard, boolean valid, String explanation,
                            String error) {
        this.index = index;
        this.shard = shard;
        this.valid = valid;
        this.explanation = explanation;
        this.error = error;
    }

    public String getIndex() {
        return this.index;
    }

    public int getShard() {
        return this.shard;
    }

    public boolean isValid() {
        return this.valid;
    }

    public String getError() {
        return this.error;
    }

    public String getExplanation() {
        return this.explanation;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(index);
        out.writeInt(shard);
        out.writeBoolean(valid);
        out.writeOptionalString(explanation);
        out.writeOptionalString(error);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (getIndex() != null) {
            builder.field(INDEX_FIELD, getIndex());
        }
        if(getShard() >= 0) {
            builder.field(SHARD_FIELD, getShard());
        }
        builder.field(VALID_FIELD, isValid());
        if (getError() != null) {
            builder.field(ERROR_FIELD, getError());
        }
        if (getExplanation() != null) {
            builder.field(EXPLANATION_FIELD, getExplanation());
        }
        return builder;
    }

    public static QueryExplanation fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryExplanation other = (QueryExplanation) o;
        return Objects.equals(getIndex(), other.getIndex()) &&
            Objects.equals(getShard(), other.getShard()) &&
            Objects.equals(isValid(), other.isValid()) &&
            Objects.equals(getError(), other.getError()) &&
            Objects.equals(getExplanation(), other.getExplanation());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getIndex(), getShard(), isValid(), getError(), getExplanation());
    }
}
