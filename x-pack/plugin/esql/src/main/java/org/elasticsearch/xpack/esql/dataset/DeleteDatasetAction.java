/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.dataset;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.esql.EsqlDatasetActionNames;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Delete a single dataset by name. No referential dependents — nothing references datasets in
 * this PR (future EXTERNAL / FROM integration reads the name at query time without a reverse index).
 */
public class DeleteDatasetAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteDatasetAction INSTANCE = new DeleteDatasetAction();
    public static final String NAME = EsqlDatasetActionNames.ESQL_DELETE_DATASET_ACTION_NAME;

    private DeleteDatasetAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> {
        private final String name;

        public Request(TimeValue masterNodeTimeout, TimeValue ackTimeout, String name) {
            super(masterNodeTimeout, ackTimeout);
            this.name = name;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.name = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
        }

        @Override
        public ActionRequestValidationException validate() {
            if (Strings.hasText(name) == false) {
                return addValidationError("dataset name is missing", null);
            }
            return null;
        }

        public String name() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(name, request.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }
    }
}
