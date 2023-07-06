/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class DeleteAnalyticsCollectionAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteAnalyticsCollectionAction INSTANCE = new DeleteAnalyticsCollectionAction();
    public static final String NAME = "cluster:admin/xpack/application/analytics/delete";

    private DeleteAnalyticsCollectionAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends MasterNodeRequest<Request> implements ToXContentObject {
        private final String collectionName;

        public static ParseField COLLECTION_NAME_FIELD = new ParseField("collection_name");

        public Request(StreamInput in) throws IOException {
            super(in);
            this.collectionName = in.readString();
        }

        public Request(String collectionName) {
            this.collectionName = collectionName;
        }

        public String getCollectionName() {
            return collectionName;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            if (Strings.isNullOrEmpty(collectionName)) {
                validationException = addValidationError("Collection name missing", validationException);
            }

            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(collectionName);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request that = (Request) o;
            return Objects.equals(collectionName, that.collectionName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(collectionName);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(COLLECTION_NAME_FIELD.getPreferredName(), collectionName);
            builder.endObject();
            return builder;
        }

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Request, Void> PARSER = new ConstructingObjectParser<>(
            "delete_analytics_collection_request",
            p -> new Request((String) p[0])
        );

        static {
            PARSER.declareString(constructorArg(), COLLECTION_NAME_FIELD);
        }

        public static Request parse(XContentParser parser) {
            return PARSER.apply(parser, null);
        }
    }
}
