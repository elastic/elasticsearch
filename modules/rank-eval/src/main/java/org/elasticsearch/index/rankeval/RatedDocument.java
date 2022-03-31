/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

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

    private static final ConstructingObjectParser<RatedDocument, Void> PARSER = new ConstructingObjectParser<>(
        "rated_document",
        a -> new RatedDocument((String) a[0], (String) a[1], (Integer) a[2])
    );

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
        return key.index();
    }

    public String getDocID() {
        return key.docId();
    }

    public int getRating() {
        return rating;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(key.index());
        out.writeString(key.docId());
        out.writeVInt(rating);
    }

    static RatedDocument fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INDEX_FIELD.getPreferredName(), key.index());
        builder.field(DOC_ID_FIELD.getPreferredName(), key.docId());
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
    record DocumentKey(String index, String docId) {

        DocumentKey {
            if (Strings.isNullOrEmpty(index)) {
                throw new IllegalArgumentException("Index must be set for each rated document");
            }
            if (Strings.isNullOrEmpty(docId)) {
                throw new IllegalArgumentException("DocId must be set for each rated document");
            }
        }

        @Override
        public String toString() {
            return "{\"_index\":\"" + index + "\",\"_id\":\"" + docId + "\"}";
        }
    }
}
