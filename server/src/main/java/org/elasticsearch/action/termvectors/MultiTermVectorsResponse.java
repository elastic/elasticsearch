/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.termvectors;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Iterator;

public class MultiTermVectorsResponse extends ActionResponse implements Iterable<MultiTermVectorsItemResponse>, ToXContentObject {

    /**
     * Represents a failure.
     */
    public static class Failure implements Writeable {
        private final String index;
        private final String id;
        private final Exception cause;

        public Failure(String index, String id, Exception cause) {
            this.index = index;
            this.id = id;
            this.cause = cause;
        }

        public Failure(StreamInput in) throws IOException {
            index = in.readString();
            if (in.getTransportVersion().before(TransportVersions.V_8_0_0)) {
                // types no longer relevant so ignore
                String type = in.readOptionalString();
                if (type != null) {
                    throw new IllegalStateException("types are no longer supported but found [" + type + "]");
                }
            }
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
            if (out.getTransportVersion().before(TransportVersions.V_8_0_0)) {
                // types not supported so send an empty array to previous versions
                out.writeOptionalString(null);
            }
            out.writeString(id);
            out.writeException(cause);
        }
    }

    private final MultiTermVectorsItemResponse[] responses;

    public MultiTermVectorsResponse(MultiTermVectorsItemResponse[] responses) {
        this.responses = responses;
    }

    public MultiTermVectorsItemResponse[] getResponses() {
        return this.responses;
    }

    @Override
    public Iterator<MultiTermVectorsItemResponse> iterator() {
        return Iterators.forArray(responses);
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
