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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents a document (specified by its _index/_id) and its corresponding rating
 * with respect to a specific search query.
 * <p>
 * The json structure of this element in a request:
 * <pre>
 * {
 *   "_index": "my_index",
 *   "_id": "doc1",
 *   "rating": 0
 * }
 * </pre>
 *
 */
public class RatedDocument implements Writeable, ToXContentObject {

    static final ParseField RATING_FIELD = new ParseField("rating");
    static final ParseField DOC_ID_FIELD = new ParseField("_id");
    static final ParseField INDEX_FIELD = new ParseField("_index");

    private static final ConstructingObjectParser<RatedDocument, Void> PARSER = new ConstructingObjectParser<>("rated_document",
            a -> new RatedDocument((String) a[0], (String) a[1], (Integer) a[2]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), INDEX_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), DOC_ID_FIELD);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), RATING_FIELD);
    }

    private final int rating;
    private final DocumentKey key;

    public RatedDocument(String index, String id, int rating) {
        this.key = new DocumentKey(index, id);
        this.rating = rating;
    }

    RatedDocument(StreamInput in) throws IOException {
        this.key = new DocumentKey(in.readString(), in.readString());
        this.rating = in.readVInt();
    }

    public DocumentKey getKey() {
        return this.key;
    }

    public String getIndex() {
        return key.getIndex();
    }

    public String getDocID() {
        return key.getDocId();
    }

    public int getRating() {
        return rating;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(key.getIndex());
        out.writeString(key.getDocId());
        out.writeVInt(rating);
    }

    static RatedDocument fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INDEX_FIELD.getPreferredName(), key.getIndex());
        builder.field(DOC_ID_FIELD.getPreferredName(), key.getDocId());
        builder.field(RATING_FIELD.getPreferredName(), rating);
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
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
        return Objects.equals(key, other.key) && Objects.equals(rating, other.rating);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(key, rating);
    }

    /**
     * a joint document key consisting of the documents index and id
     */
    static class DocumentKey {

        private final String docId;
        private final String index;

        DocumentKey(String index, String docId) {
            if (Strings.isNullOrEmpty(index)) {
                throw new IllegalArgumentException("Index must be set for each rated document");
            }
            if (Strings.isNullOrEmpty(docId)) {
                throw new IllegalArgumentException("DocId must be set for each rated document");
            }

            this.index = index;
            this.docId = docId;
        }

        String getIndex() {
            return index;
        }

        String getDocId() {
            return docId;
        }

        @Override
        public final boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            DocumentKey other = (DocumentKey) obj;
            return Objects.equals(index, other.index) && Objects.equals(docId, other.docId);
        }

        @Override
        public final int hashCode() {
            return Objects.hash(index, docId);
        }

        @Override
        public String toString() {
            return "{\"_index\":\"" + index + "\",\"_id\":\"" + docId + "\"}";
        }
    }
}
