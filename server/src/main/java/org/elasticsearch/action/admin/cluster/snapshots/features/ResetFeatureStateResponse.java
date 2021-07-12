/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.features;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** Response to a feature state reset request. */
public class ResetFeatureStateResponse extends ActionResponse implements ToXContentObject {

    List<ResetFeatureStateStatus> resetFeatureStateStatusList;

    /**
     * Create a response showing which features have had state reset and success
     * or failure status.
     *
     * @param resetFeatureStateStatuses A list of status responses
     */
    public ResetFeatureStateResponse(List<ResetFeatureStateStatus> resetFeatureStateStatuses) {
        resetFeatureStateStatusList = new ArrayList<>();
        resetFeatureStateStatusList.addAll(resetFeatureStateStatuses);
        resetFeatureStateStatusList.sort(Comparator.comparing(ResetFeatureStateStatus::getFeatureName));
    }

    public ResetFeatureStateResponse(StreamInput in) throws IOException {
        super(in);
        this.resetFeatureStateStatusList = in.readList(ResetFeatureStateStatus::new);
    }

    /**
     * @return List of statuses for individual reset operations, one per feature that we tried to reset
     */
    public List<ResetFeatureStateStatus> getFeatureStateResetStatuses() {
        return Collections.unmodifiableList(this.resetFeatureStateStatusList);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.startArray("features");
            for (ResetFeatureStateStatus resetFeatureStateStatus : this.resetFeatureStateStatusList) {
                builder.value(resetFeatureStateStatus);
            }
            builder.endArray();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(this.resetFeatureStateStatusList);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResetFeatureStateResponse that = (ResetFeatureStateResponse) o;
        return Objects.equals(resetFeatureStateStatusList, that.resetFeatureStateStatusList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resetFeatureStateStatusList);
    }

    @Override
    public String toString() {
        return "ResetFeatureStateResponse{" + "resetFeatureStateStatusList=" + resetFeatureStateStatusList + '}';
    }

    /**
     * An object with the name of a feature and a message indicating success or
     * failure.
     */
    public static class ResetFeatureStateStatus implements Writeable, ToXContentObject {
        private final String featureName;
        private final Status status;
        private final Exception exception;

        /**
         * Success or failure enum. Not a boolean so that we can easily display
         * "SUCCESS" or "FAILURE" when this object is serialized.
         */
        public enum Status {
            SUCCESS,
            FAILURE
        }

        /**
         * Create a feature status for a successful reset operation
         * @param featureName Name of the feature whose state was successfully reset
         * @return Success status for a feature
         */
        public static ResetFeatureStateStatus success(String featureName) {
            return new ResetFeatureStateStatus(featureName, Status.SUCCESS, null);
        }

        /**
         * Create a feature status for a failed reset operation
         * @param featureName Name of the feature that failed
         * @param exception The exception that caused or described the failure
         * @return Failure status for a feature
         */
        public static ResetFeatureStateStatus failure(String featureName, Exception exception) {
            return new ResetFeatureStateStatus(featureName, Status.FAILURE, exception);
        }

        private ResetFeatureStateStatus(String featureName, Status status, @Nullable Exception exception) {
            this.featureName = featureName;
            this.status = status;
            assert Status.FAILURE.equals(status) ? Objects.nonNull(exception) : Objects.isNull(exception);
            this.exception = exception;
        }

        ResetFeatureStateStatus(StreamInput in) throws IOException {
            this.featureName = in.readString();
            this.status = Status.valueOf(in.readString());
            this.exception = in.readBoolean() ? in.readException() : null;
        }

        /**
         * @return Name of the feature we tried to reset
         */
        public String getFeatureName() {
            return this.featureName;
        }

        /**
         * @return Success or failure for the reset operation
         */
        public Status getStatus() {
            return this.status;
        }

        /**
         * @return For a failed reset operation, the exception that caused or describes the failure.
         */
        @Nullable
        public Exception getException() {
            return this.exception;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("feature_name", this.featureName);
            builder.field("status", this.status);
            if (Objects.nonNull(this.exception)) {
                builder.field("exception");
                builder.startObject();
                new ElasticsearchException(exception).toXContent(builder, params);
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }

        /**
         * Without a convenient way to compare Exception equality, we consider
         * only feature name and success or failure for equality.
         * @param o An object to compare for equality
         * @return True if the feature name and status are equal, false otherwise
         */
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ResetFeatureStateStatus that = (ResetFeatureStateStatus) o;
            return Objects.equals(featureName, that.featureName) && status == that.status;
        }

        /**
         * @return Hash code based only on feature name and status.
         */
        @Override
        public int hashCode() {
            return Objects.hash(featureName, status);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(this.featureName);
            out.writeString(this.status.toString());
            if (exception != null) {
                out.writeBoolean(true);
                out.writeException(exception);
            } else {
                out.writeBoolean(false);
            }
        }

        @Override
        public String toString() {
            return "ResetFeatureStateStatus{"
                + "featureName='"
                + featureName
                + '\''
                + ", status="
                + status
                + ", exception='"
                + exception
                + '\''
                + '}';
        }
    }
}
