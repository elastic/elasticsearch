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

package org.elasticsearch.index.rankeval;

import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * A document ID and its rating for the query QA use case.
 * */
public class RatedDocument extends ToXContentToBytes implements Writeable {

    public static final ParseField DOC_ID_FIELD = new ParseField("doc_id");
    public static final ParseField TYPE_FIELD = new ParseField("type");
    public static final ParseField INDEX_FIELD = new ParseField("index");
    public static final ParseField RATING_FIELD = new ParseField("rating");

    private static final ObjectParser<RatedDocument, RankEvalContext> PARSER = new ObjectParser<>("ratings", RatedDocument::new);

    static {
        PARSER.declareString(RatedDocument::setIndex, INDEX_FIELD);
        PARSER.declareString(RatedDocument::setType, TYPE_FIELD);
        PARSER.declareString(RatedDocument::setDocId, DOC_ID_FIELD);
        PARSER.declareInt(RatedDocument::setRating, RATING_FIELD);
    }

    // TODO instead of docId use path to id and id itself
    private String docId;
    private String type;
    private String index;
    private int rating;

    RatedDocument() {}

    void setIndex(String index) {
        this.index = index;
    }

    void setType(String type) {
        this.type = type;
    }
    
    void setDocId(String docId) {
        this.docId = docId;
    }
   
    void setRating(int rating) {
        this.rating = rating;
    }

    public RatedDocument(String index, String type, String docId, int rating) {
        this.index = index;
        this.type = type;
        this.docId = docId;
        this.rating = rating;
    }

    public RatedDocument(StreamInput in) throws IOException {
        this.index = in.readString();
        this.type = in.readString();
        this.docId = in.readString();
        this.rating = in.readVInt();
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public String getDocID() {
        return docId;
    }

    public int getRating() {
        return rating;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeString(type);
        out.writeString(docId);
        out.writeVInt(rating);
    }

    public static RatedDocument fromXContent(XContentParser parser, RankEvalContext context) throws IOException {
        return PARSER.parse(parser, context);
    }
    
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INDEX_FIELD.getPreferredName(), index);
        builder.field(TYPE_FIELD.getPreferredName(), type);
        builder.field(DOC_ID_FIELD.getPreferredName(), docId);
        builder.field(RATING_FIELD.getPreferredName(), rating);
        builder.endObject();
        return builder;
    }
    
    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RatedDocument other = (RatedDocument) obj;
        return Objects.equals(index, other.index) &&
                Objects.equals(type, other.type) &&
                Objects.equals(docId, other.docId) &&
                Objects.equals(rating, other.rating);
    }
    
    @Override
    public final int hashCode() {
        return Objects.hash(getClass(), index, type, docId, rating);
    }
}
