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
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * A document ID and its rating for the query QA use case.
 * */
public class RatedDocument extends ToXContentToBytes implements Writeable {

    public static final ParseField RATING_FIELD = new ParseField("rating");
    public static final ParseField DOC_ID_FIELD = new ParseField("_id");
    public static final ParseField TYPE_FIELD = new ParseField("_type");
    public static final ParseField INDEX_FIELD = new ParseField("_index");

    private static final ConstructingObjectParser<RatedDocument, Void> PARSER =
            new ConstructingObjectParser<>("rated_document",
            a -> new RatedDocument((String) a[0], (String) a[1], (String) a[2], (Integer) a[3]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), INDEX_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), TYPE_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), DOC_ID_FIELD);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), RATING_FIELD);
    }

    private int rating;
    private DocumentKey key;

    public RatedDocument(String index, String type, String docId, int rating) {
        this(new DocumentKey(index, type, docId), rating);
    }

    public RatedDocument(StreamInput in) throws IOException {
        this.key = new DocumentKey(in);
        this.rating = in.readVInt();
    }

    public RatedDocument(DocumentKey ratedDocumentKey, int rating) {
        this.key = ratedDocumentKey;
        this.rating = rating;
    }

    public DocumentKey getKey() {
        return this.key;
    }

    public String getIndex() {
        return key.getIndex();
    }

    public String getType() {
        return key.getType();
    }

    public String getDocID() {
        return key.getDocID();
    }

    public int getRating() {
        return rating;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        this.key.writeTo(out);
        out.writeVInt(rating);
    }

    public static RatedDocument fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INDEX_FIELD.getPreferredName(), key.getIndex());
        builder.field(TYPE_FIELD.getPreferredName(), key.getType());
        builder.field(DOC_ID_FIELD.getPreferredName(), key.getDocID());
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
        return Objects.equals(key, other.key) &&
                Objects.equals(rating, other.rating);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(key, rating);
    }
}
