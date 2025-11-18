/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.indices.readonly;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A response for a remove index block action.
 */
public class RemoveIndexBlockResponse extends AcknowledgedResponse {

    public static final RemoveIndexBlockResponse EMPTY = new RemoveIndexBlockResponse(true, List.of());

    private final List<RemoveBlockResult> results;

    public RemoveIndexBlockResponse(StreamInput in) throws IOException {
        super(in);
        results = in.readCollectionAsImmutableList(RemoveBlockResult::new);
    }

    public RemoveIndexBlockResponse(boolean acknowledged, List<RemoveBlockResult> results) {
        super(acknowledged);
        this.results = List.copyOf(Objects.requireNonNull(results, "results must not be null"));
    }

    /**
     * Returns the list of {@link RemoveBlockResult}.
     */
    public List<RemoveBlockResult> getResults() {
        return results;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeCollection(results);
    }

    @Override
    protected void addCustomFields(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("indices");
        for (RemoveBlockResult result : results) {
            result.toXContent(builder, params);
        }
        builder.endArray();
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static class RemoveBlockResult implements Writeable, ToXContentObject {

        private final Index index;
        private final @Nullable Exception exception;

        public RemoveBlockResult(final Index index) {
            this.index = Objects.requireNonNull(index);
            this.exception = null;
        }

        public RemoveBlockResult(final Index index, final Exception failure) {
            this.index = Objects.requireNonNull(index);
            this.exception = Objects.requireNonNull(failure);
        }

        RemoveBlockResult(final StreamInput in) throws IOException {
            this.index = new Index(in);
            this.exception = in.readException();
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            index.writeTo(out);
            out.writeException(exception);
        }

        public Index getIndex() {
            return index;
        }

        public Exception getException() {
            return exception;
        }

        public boolean hasFailures() {
            return exception != null;
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject();
            {
                builder.field("name", index.getName());
                if (hasFailures()) {
                    builder.startObject("exception");
                    ElasticsearchException.generateFailureXContent(builder, params, exception, true);
                    builder.endObject();
                } else {
                    builder.field("unblocked", true);
                }
            }
            return builder.endObject();
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

}
