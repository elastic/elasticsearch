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
import org.elasticsearch.common.ParsingException;
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

    public static final ParseField RATING_FIELD = new ParseField("rating");
    public static final ParseField KEY_FIELD = new ParseField("key");

    private static final ObjectParser<RatedDocument, RankEvalContext> PARSER = new ObjectParser<>("ratings", RatedDocument::new);

    static {
        PARSER.declareObject(RatedDocument::setKey, (p, c) -> {
            try {
                return RatedDocumentKey.fromXContent(p, c);
            } catch (IOException ex) {
                throw new ParsingException(p.getTokenLocation(), "error parsing rank request", ex);
            }
        } , KEY_FIELD);
        PARSER.declareInt(RatedDocument::setRating, RATING_FIELD);
    }

    private RatedDocumentKey key;
    private int rating;

    RatedDocument() {}

    void setRatedDocumentKey(RatedDocumentKey key) {
        this.key = key;
    }

    void setKey(RatedDocumentKey key) {
        this.key = key;
    }

    void setRating(int rating) {
        this.rating = rating;
    }

    public RatedDocument(RatedDocumentKey key, int rating) {
        this.key = key;
        this.rating = rating;
    }

    public RatedDocument(StreamInput in) throws IOException {
        this.key = new RatedDocumentKey(in);
        this.rating = in.readVInt();
    }

    public RatedDocumentKey getKey() {
        return this.key;
    }

    public int getRating() {
        return rating;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        this.key.writeTo(out);
        out.writeVInt(rating);
    }

    public static RatedDocument fromXContent(XContentParser parser, RankEvalContext context) throws IOException {
        return PARSER.parse(parser, context);
    }
    
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(KEY_FIELD.getPreferredName(), key);
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
        return Objects.hash(getClass(), key, rating);
    }
}
