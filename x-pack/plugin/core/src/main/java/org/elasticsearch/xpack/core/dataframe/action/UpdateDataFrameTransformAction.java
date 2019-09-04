/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfigUpdate;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.cluster.metadata.MetaDataCreateIndexService.validateIndexOrAliasName;

public class UpdateDataFrameTransformAction extends ActionType<UpdateDataFrameTransformAction.Response> {

    public static final UpdateDataFrameTransformAction INSTANCE = new UpdateDataFrameTransformAction();
    public static final String NAME = "cluster:admin/data_frame/update";

    private static final TimeValue MIN_FREQUENCY = TimeValue.timeValueSeconds(1);
    private static final TimeValue MAX_FREQUENCY = TimeValue.timeValueHours(1);

    private UpdateDataFrameTransformAction() {
        super(NAME, Response::new);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        private final DataFrameTransformConfigUpdate update;
        private final String id;
        private final boolean deferValidation;

        public Request(DataFrameTransformConfigUpdate update, String id, boolean deferValidation)  {
            this.update = update;
            this.id = id;
            this.deferValidation = deferValidation;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.update = new DataFrameTransformConfigUpdate(in);
            this.id = in.readString();
            this.deferValidation = in.readBoolean();
        }

        public static Request fromXContent(final XContentParser parser, final String id, final boolean deferValidation) {
            return new Request(DataFrameTransformConfigUpdate.fromXContent(parser), id, deferValidation);
        }

        /**
         * More complex validations with how {@link DataFrameTransformConfig#getDestination()} and
         * {@link DataFrameTransformConfig#getSource()} relate are done in the update transport handler.
         */
        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (update.getDestination() != null && update.getDestination().getIndex() != null) {
                String destIndex = update.getDestination().getIndex();
                try {
                    validateIndexOrAliasName(destIndex, InvalidIndexNameException::new);
                    if (!destIndex.toLowerCase(Locale.ROOT).equals(destIndex)) {
                        validationException = addValidationError("dest.index [" + destIndex + "] must be lowercase", validationException);
                    }
                } catch (InvalidIndexNameException ex) {
                    validationException = addValidationError(ex.getMessage(), validationException);
                }
            }
            TimeValue frequency = update.getFrequency();
            if (frequency != null) {
                if (frequency.compareTo(MIN_FREQUENCY) < 0) {
                    validationException = addValidationError(
                        "minimum permitted [" + DataFrameField.FREQUENCY + "] is [" + MIN_FREQUENCY.getStringRep() + "]",
                        validationException);
                } else if (frequency.compareTo(MAX_FREQUENCY) > 0) {
                    validationException = addValidationError(
                        "highest permitted [" + DataFrameField.FREQUENCY + "] is [" + MAX_FREQUENCY.getStringRep() + "]",
                        validationException);
                }
            }

            return validationException;
        }

        public String getId() {
            return id;
        }

        public boolean isDeferValidation() {
            return deferValidation;
        }

        public DataFrameTransformConfigUpdate getUpdate() {
            return update;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            this.update.writeTo(out);
            out.writeString(id);
            out.writeBoolean(deferValidation);
        }

        @Override
        public int hashCode() {
            return Objects.hash(update, id, deferValidation);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(update, other.update) &&
                this.deferValidation == other.deferValidation &&
                this.id.equals(other.id);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final DataFrameTransformConfig config;

        public Response(DataFrameTransformConfig config) {
            this.config = config;
        }

        public Response(StreamInput in) throws IOException {
            this.config = new DataFrameTransformConfig(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            this.config.writeTo(out);
        }

        @Override
        public int hashCode() {
            return config.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Response other = (Response) obj;
            return Objects.equals(config, other.config);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return config.toXContent(builder, params);
        }
    }
}
