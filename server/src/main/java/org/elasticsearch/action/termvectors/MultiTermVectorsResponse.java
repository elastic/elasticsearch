/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.termvectors;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

public class MultiTermVectorsResponse extends ActionResponse implements Iterable<MultiTermVectorsItemResponse>, ToXContentObject {

    /**
     * Represents a failure.
     */
    public static class Failure implements Writeable {
        private final String index;
        private final String type;
        private final String id;
        private final Exception cause;

        public Failure(String index, String type, String id, Exception cause) {
            this.index = index;
            this.type = type;
            this.id = id;
            this.cause = cause;
        }

        public Failure(StreamInput in) throws IOException {
            index = in.readString();
            type = in.readOptionalString();
            id = in.readString();
            cause = in.readException();
        }

        /**
         * The index name of the action.
         */
        public String getIndex() {
            return this.index;
        }

        /**
         * The type of the action.
         *
         * @deprecated Types are in the process of being removed.
         */
        @Deprecated
        public String getType() {
            return type;
        }

        /**
         * The id of the action.
         */
        public String getId() {
            return id;
        }

        /**
         * The failure cause.
         */
        public Exception getCause() {
            return this.cause;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            out.writeOptionalString(type);
            out.writeString(id);
            out.writeException(cause);
        }
    }

    private final MultiTermVectorsItemResponse[] responses;

    public MultiTermVectorsResponse(MultiTermVectorsItemResponse[] responses) {
        this.responses = responses;
    }

    public MultiTermVectorsResponse(StreamInput in) throws IOException {
        super(in);
        responses = in.readArray(MultiTermVectorsItemResponse::new, MultiTermVectorsItemResponse[]::new);
    }

    public MultiTermVectorsItemResponse[] getResponses() {
        return this.responses;
    }

    @Override
    public Iterator<MultiTermVectorsItemResponse> iterator() {
        return Arrays.stream(responses).iterator();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(Fields.DOCS);
        for (MultiTermVectorsItemResponse response : responses) {
            if (response.isFailed()) {
                builder.startObject();
                Failure failure = response.getFailure();
                builder.field(Fields._INDEX, failure.getIndex());
                builder.field(Fields._TYPE, failure.getType());
                builder.field(Fields._ID, failure.getId());
                ElasticsearchException.generateFailureXContent(builder, params, failure.getCause(), true);
                builder.endObject();
            } else {
                TermVectorsResponse getResponse = response.getResponse();
                getResponse.toXContent(builder, params);
            }
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String DOCS = "docs";
        static final String _INDEX = "_index";
        static final String _TYPE = "_type";
        static final String _ID = "_id";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(responses);
    }
}
