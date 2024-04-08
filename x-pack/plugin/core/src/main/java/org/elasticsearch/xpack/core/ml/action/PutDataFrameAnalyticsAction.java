/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.common.validation.SourceDestValidator;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsSource;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.MlStrings;

import java.io.IOException;
import java.util.Objects;

public class PutDataFrameAnalyticsAction extends ActionType<PutDataFrameAnalyticsAction.Response> {

    public static final PutDataFrameAnalyticsAction INSTANCE = new PutDataFrameAnalyticsAction();
    public static final String NAME = "cluster:admin/xpack/ml/data_frame/analytics/put";

    private PutDataFrameAnalyticsAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject {

        /**
         * Parses request.
         */
        public static Request parseRequest(String id, XContentParser parser) {
            DataFrameAnalyticsConfig.Builder config = DataFrameAnalyticsConfig.STRICT_PARSER.apply(parser, null);
            if (config.getId() == null) {
                config.setId(id);
            } else if (Strings.isNullOrEmpty(id) == false && id.equals(config.getId()) == false) {
                // If we have both URI and body ID, they must be identical
                throw new IllegalArgumentException(
                    Messages.getMessage(Messages.INCONSISTENT_ID, DataFrameAnalyticsConfig.ID, config.getId(), id)
                );
            }

            return new PutDataFrameAnalyticsAction.Request(config.build());
        }

        private final DataFrameAnalyticsConfig config;

        public Request(StreamInput in) throws IOException {
            super(in);
            config = new DataFrameAnalyticsConfig(in);
        }

        public Request(DataFrameAnalyticsConfig config) {
            this.config = config;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            config.writeTo(out);
        }

        public DataFrameAnalyticsConfig getConfig() {
            return config;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException error = null;
            error = checkConfigIdIsValid(config, error);
            error = SourceDestValidator.validateRequest(error, config.getDest().getIndex());
            error = checkNoIncludedAnalyzedFieldsAreExcludedBySourceFiltering(config, error);
            return error;
        }

        private static ActionRequestValidationException checkConfigIdIsValid(
            DataFrameAnalyticsConfig analyticsConfig,
            ActionRequestValidationException error
        ) {
            if (MlStrings.isValidId(analyticsConfig.getId()) == false) {
                error = ValidateActions.addValidationError(
                    Messages.getMessage(Messages.INVALID_ID, DataFrameAnalyticsConfig.ID, analyticsConfig.getId()),
                    error
                );
            }
            if (MlStrings.hasValidLengthForId(analyticsConfig.getId()) == false) {
                error = ValidateActions.addValidationError(
                    Messages.getMessage(
                        Messages.ID_TOO_LONG,
                        DataFrameAnalyticsConfig.ID,
                        analyticsConfig.getId(),
                        MlStrings.ID_LENGTH_LIMIT
                    ),
                    error
                );
            }
            return error;
        }

        private static ActionRequestValidationException checkNoIncludedAnalyzedFieldsAreExcludedBySourceFiltering(
            DataFrameAnalyticsConfig analyticsConfig,
            ActionRequestValidationException error
        ) {
            if (analyticsConfig.getAnalyzedFields() == null) {
                return error;
            }
            for (String analyzedInclude : analyticsConfig.getAnalyzedFields().includes()) {
                if (analyticsConfig.getSource().isFieldExcluded(analyzedInclude)) {
                    return ValidateActions.addValidationError(
                        "field ["
                            + analyzedInclude
                            + "] is included in ["
                            + DataFrameAnalyticsConfig.ANALYZED_FIELDS.getPreferredName()
                            + "] but not in ["
                            + DataFrameAnalyticsConfig.SOURCE.getPreferredName()
                            + "."
                            + DataFrameAnalyticsSource._SOURCE.getPreferredName()
                            + "]",
                        error
                    );
                }
            }
            return error;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            config.toXContent(builder, params);
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PutDataFrameAnalyticsAction.Request request = (PutDataFrameAnalyticsAction.Request) o;
            return Objects.equals(config, request.config);
        }

        @Override
        public int hashCode() {
            return Objects.hash(config);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private DataFrameAnalyticsConfig config;

        public Response(DataFrameAnalyticsConfig config) {
            this.config = config;
        }

        Response() {}

        public Response(StreamInput in) throws IOException {
            super(in);
            config = new DataFrameAnalyticsConfig(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            config.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            config.toXContent(builder, params);
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(config, response.config);
        }

        @Override
        public int hashCode() {
            return Objects.hash(config);
        }
    }
}
