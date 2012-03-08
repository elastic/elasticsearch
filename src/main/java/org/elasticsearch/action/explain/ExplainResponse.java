/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import java.io.IOException;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentFactory;

/**
 * A response of an explain request.
 */
public class ExplainResponse implements ActionResponse, ToXContent {

    private String index;
    private String type;
    private String id;
    private boolean exists;
    private Explanation explanation;

    public ExplainResponse() {
    }

    public ExplainResponse(String index, String type, String id, boolean exists, Explanation explanation) {
        this.index = index;
        this.type = type;
        this.id = id;
        this.exists = exists;
        this.explanation = explanation;
    }

    public String getIndex() {
        return index;
    }

    public String index() {
        return index;
    }

    public Explanation getExplanation() {
        return explanation;
    }

    public Explanation explanation() {
        return explanation;
    }

    public String getId() {
        return id;
    }

    public String id() {
        return id;
    }

    public String getType() {
        return type;
    }

    public String type() {
        return type;
    }

    public static class Fields {
        static final XContentBuilderString _INDEX = new XContentBuilderString("_index");
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString _ID = new XContentBuilderString("_id");
        static final XContentBuilderString EXISTS = new XContentBuilderString("exists");
        static final XContentBuilderString EXPLANATION = new XContentBuilderString("explanation");
        static final XContentBuilderString VALUE = new XContentBuilderString("value");
        static final XContentBuilderString DESCRIPTION = new XContentBuilderString("description");
        static final XContentBuilderString DETAILS = new XContentBuilderString("details");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Fields._INDEX, index);
        builder.field(Fields._TYPE, type);
        builder.field(Fields._ID, id);
        builder.field(Fields.EXISTS, exists);
        if (explanation != null) {
            builder.field(Fields.EXPLANATION);
            buildExplanation(builder, explanation);
        }
        builder.endObject();
        return builder;
    }

    private void buildExplanation(XContentBuilder builder, Explanation explanation) throws IOException {
        builder.startObject();
        builder.field(Fields.VALUE, explanation.getValue());
        builder.field(Fields.DESCRIPTION, explanation.getDescription());
        Explanation[] innerExps = explanation.getDetails();
        if (innerExps != null) {
            builder.startArray(Fields.DETAILS);
            for (Explanation exp : innerExps) {
                buildExplanation(builder, exp);
            }
            builder.endArray();
        }
        builder.endObject();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            explanation = Lucene.readExplanation(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (explanation == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Lucene.writeExplanation(out, explanation);
        }
    }

    @Override
    public String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return builder.string();
        } catch (IOException e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }

}
