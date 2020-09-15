/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class DeleteDataFrameAnalyticsAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteDataFrameAnalyticsAction INSTANCE = new DeleteDataFrameAnalyticsAction();
    public static final String NAME = "cluster:admin/xpack/ml/data_frame/analytics/delete";
    public static final String DELETION_TASK_DESCRIPTION_PREFIX = "delete-analytics-";

    private DeleteDataFrameAnalyticsAction() {
        super(NAME, AcknowledgedResponse::new);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        public static final ParseField FORCE = new ParseField("force");
        public static final ParseField TIMEOUT = new ParseField("timeout");

        // Default timeout matches that of delete by query
        private static final TimeValue DEFAULT_TIMEOUT = new TimeValue(1, TimeUnit.MINUTES);

        private String id;
        private boolean force;

        public Request(StreamInput in) throws IOException {
            super(in);
            id = in.readString();
            force = in.readBoolean();
        }

        public Request() {
            timeout(DEFAULT_TIMEOUT);
        }

        public Request(String id) {
            this();
            this.id = ExceptionsHelper.requireNonNull(id, DataFrameAnalyticsConfig.ID);
        }

        public String getId() {
            return id;
        }

        public boolean isForce() {
            return force;
        }

        public void setForce(boolean force) {
            this.force = force;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public String getDescription() {
            return DELETION_TASK_DESCRIPTION_PREFIX + id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DeleteDataFrameAnalyticsAction.Request request = (DeleteDataFrameAnalyticsAction.Request) o;
            return Objects.equals(id, request.id)
                && force == request.force
                && Objects.equals(timeout, request.timeout);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(id);
            out.writeBoolean(force);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, force, timeout);
        }
    }
}
