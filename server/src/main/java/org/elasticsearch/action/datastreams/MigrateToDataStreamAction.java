/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.datastreams;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.Objects;

public class MigrateToDataStreamAction extends ActionType<AcknowledgedResponse> {

    public static final MigrateToDataStreamAction INSTANCE = new MigrateToDataStreamAction();
    public static final String NAME = "indices:admin/data_stream/migrate";

    private MigrateToDataStreamAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<MigrateToDataStreamAction.Request> implements IndicesRequest {

        private final String aliasName;

        public Request(TimeValue masterNodeTimeout, TimeValue ackTimeout, String aliasName) {
            super(masterNodeTimeout, ackTimeout);
            this.aliasName = aliasName;
        }

        public String getAliasName() {
            return aliasName;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (Strings.hasText(aliasName) == false) {
                validationException = ValidateActions.addValidationError("name is missing", validationException);
            }
            return validationException;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.aliasName = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(aliasName);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MigrateToDataStreamAction.Request request = (MigrateToDataStreamAction.Request) o;
            return aliasName.equals(request.aliasName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(aliasName);
        }

        @Override
        public String[] indices() {
            return new String[] { aliasName };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
        }
    }

}
