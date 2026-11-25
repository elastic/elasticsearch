/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.get;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Iterator;

public class MultiGetResponse extends ActionResponse implements Iterable<MultiGetItemResponse>, ChunkedToXContentObject {

    static final ParseField INDEX = new ParseField("_index");
    static final ParseField ID = new ParseField("_id");
    static final ParseField DOCS = new ParseField("docs");

    /**
     * Represents a failure.
     */
    public static class Failure implements Writeable, ToXContentObject {

        private final String index;
        private final String id;
        private final Exception exception;

        public Failure(String index, String id, Exception exception) {
            this.index = index;
            this.id = id;
            this.exception = exception;
        }

        Failure(StreamInput in) throws IOException {
            index = in.readString();
            id = in.readString();
            exception = in.readException();
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
         * The failure message.
         */
        public String getMessage() {
            return exception != null ? exception.getMessage() : null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            out.writeString(id);
            out.writeException(exception);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(INDEX.getPreferredName(), index);
            builder.field(ID.getPreferredName(), id);
            ElasticsearchException.generateFailureXContent(builder, params, exception, true);
            builder.endObject();
            return builder;
        }

        public Exception getFailure() {
            return exception;
        }
    }

    private final MultiGetItemResponse[] responses;

    public MultiGetResponse(MultiGetItemResponse[] responses) {
        this.responses = responses;
    }

    public MultiGetItemResponse[] getResponses() {
        return this.responses;
    }

    @Override
    public Iterator<MultiGetItemResponse> iterator() {
        return Iterators.forArray(responses);
    }

    /**
     * The returned iterator drops each item once it has handed it out, so a response that is still being written doesn't keep the
     * documents it has serialized reachable, unless something else references this response.
     */
    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        final MultiGetItemResponse[] unserialized = responses.clone();
        return Iterators.concat(
            ChunkedToXContentHelper.startObject(),
            ChunkedToXContentHelper.array(DOCS.getPreferredName(), Iterators.forRange(0, unserialized.length, i -> {
                final MultiGetItemResponse response = unserialized[i];
                unserialized[i] = null;
                return (b, p) -> {
                    if (response.isFailed()) {
                        response.getFailure().toXContent(b, p);
                    } else {
                        response.getResponse().toXContent(b, p);
                    }
                    return b;
                };
            })),
            ChunkedToXContentHelper.endObject()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(responses);
    }
}
