/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.analytics.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class DeleteAnalyticsCollectionAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteAnalyticsCollectionAction INSTANCE = new DeleteAnalyticsCollectionAction();
    public static final String NAME = "cluster:admin/analytics/delete";

    private DeleteAnalyticsCollectionAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends ActionRequest {
        private final String collectionId;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.collectionId = in.readString();
        }

        public Request(String collectionId) {
            this.collectionId = collectionId;
        }

        public String getCollectionId() {
            return collectionId;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            if (this.collectionId == null || this.collectionId.isEmpty()) {
                validationException = addValidationError("collectionId missing", validationException);
            }

            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(collectionId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request that = (Request) o;
            return Objects.equals(collectionId, that.collectionId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(collectionId);
        }

    }

}
