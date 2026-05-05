/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.esql.EsqlDataSourceActionNames;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class DeleteDataSourceAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteDataSourceAction INSTANCE = new DeleteDataSourceAction();
    public static final String NAME = EsqlDataSourceActionNames.ESQL_DELETE_DATA_SOURCE_ACTION_NAME;

    private DeleteDataSourceAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> {
        private String[] names;

        public Request(TimeValue masterNodeTimeout, TimeValue ackTimeout, String[] names) {
            super(masterNodeTimeout, ackTimeout);
            this.names = Objects.requireNonNull(names, "names cannot be null");
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.names = in.readStringArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(names);
        }

        @Override
        public ActionRequestValidationException validate() {
            if (CollectionUtils.isEmpty(names)) {
                return addValidationError("data source names cannot be empty", null);
            }
            return null;
        }

        public String[] names() {
            return names;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Arrays.equals(names, request.names);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(names);
        }
    }
}
