/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public final class BulkUpdateApiKeyResponse extends ActionResponse implements ToXContentObject, Writeable {

    private final List<String> updated;
    private final List<String> noops;
    private final Map<String, Exception> errorDetails;

    public BulkUpdateApiKeyResponse(final List<String> updated, final List<String> noops, final Map<String, Exception> errorDetails) {
        this.updated = updated;
        this.noops = noops;
        this.errorDetails = errorDetails;
    }

    public BulkUpdateApiKeyResponse(StreamInput in) throws IOException {
        super(in);
        this.updated = in.readStringList();
        this.noops = in.readStringList();
        this.errorDetails = in.readMap(StreamInput::readString, StreamInput::readException);
    }

    public List<String> getUpdated() {
        return updated;
    }

    public List<String> getNoops() {
        return noops;
    }

    public Map<String, Exception> getErrorDetails() {
        return errorDetails;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
            .stringListField("updated", updated)
            .stringListField("noops", noops)
            .startObject("errors")
            .field("count", errorDetails.size());
        if (errorDetails.isEmpty() == false) {
            builder.startObject("details");
            for (Map.Entry<String, Exception> idWithException : errorDetails.entrySet()) {
                builder.startObject(idWithException.getKey());
                ElasticsearchException.generateThrowableXContent(builder, params, idWithException.getValue());
                builder.endObject();
            }
            builder.endObject();
        }
        return builder.endObject() // errors
            .endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(updated);
        out.writeStringCollection(noops);
        out.writeMap(errorDetails, StreamOutput::writeString, StreamOutput::writeException);
    }

    @Override
    public String toString() {
        return "BulkUpdateApiKeyResponse{" + "updated=" + updated + ", noops=" + noops + ", errorDetails=" + errorDetails + '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final List<String> updated;
        private final List<String> noops;
        private final Map<String, Exception> errorDetails;

        public Builder() {
            updated = new ArrayList<>();
            noops = new ArrayList<>();
            errorDetails = new HashMap<>();
        }

        public Builder update(final String id) {
            updated.add(id);
            return this;
        }

        public Builder noop(final String id) {
            noops.add(id);
            return this;
        }

        public Builder error(final String id, final Exception ex) {
            errorDetails.put(id, ex);
            return this;
        }

        public BulkUpdateApiKeyResponse build() {
            return new BulkUpdateApiKeyResponse(updated, noops, errorDetails);
        }
    }

    public static BulkUpdateApiKeyResponse fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<BulkUpdateApiKeyResponse, Void> PARSER = new ConstructingObjectParser<>(
        "bulk_update_api_key_response",
        args -> new BulkUpdateApiKeyResponse((List<String>) args[0], (List<String>) args[1], ((ErrorDetails) args[2]).details())
    );
    static {
        PARSER.declareStringArray(constructorArg(), new ParseField("updated"));
        PARSER.declareStringArray(constructorArg(), new ParseField("noops"));
        PARSER.declareObject(constructorArg(), (p, c) -> ErrorDetails.fromXContent(p), new ParseField("errors"));
    }

    private record ErrorDetails(int count, Map<String, Exception> details) {
        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<ErrorDetails, Void> PARSER = new ConstructingObjectParser<>(
            "bulk_update_api_key_response_errors",
            args -> new ErrorDetails((Integer) args[0], args[1] == null ? Map.of() : (Map<String, Exception>) args[1])
        );
        static {
            PARSER.declareInt(constructorArg(), new ParseField("count"));
            PARSER.declareObject(
                optionalConstructorArg(),
                // TODO validate this is acceptable
                (p, c) -> p.map(HashMap::new, ElasticsearchException::fromXContent),
                new ParseField("details")
            );
        }

        private static ErrorDetails fromXContent(final XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }
}
