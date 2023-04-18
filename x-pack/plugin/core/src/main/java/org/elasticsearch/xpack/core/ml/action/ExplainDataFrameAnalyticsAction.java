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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.common.validation.SourceDestValidator;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsSource;
import org.elasticsearch.xpack.core.ml.dataframe.explain.FieldSelection;
import org.elasticsearch.xpack.core.ml.dataframe.explain.MemoryEstimation;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.MlStrings;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;

public class ExplainDataFrameAnalyticsAction extends ActionType<ExplainDataFrameAnalyticsAction.Response> {

    public static final ExplainDataFrameAnalyticsAction INSTANCE = new ExplainDataFrameAnalyticsAction();
    public static final String NAME = "cluster:admin/xpack/ml/data_frame/analytics/explain";

    private ExplainDataFrameAnalyticsAction() {
        super(NAME, ExplainDataFrameAnalyticsAction.Response::new);
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject {
        public static Request parseRequest(XContentParser parser) {
            DataFrameAnalyticsConfig.Builder configBuilder = DataFrameAnalyticsConfig.STRICT_PARSER.apply(parser, null);
            DataFrameAnalyticsConfig config = configBuilder.buildForExplain();
            return new Request(config);
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

        private ActionRequestValidationException checkConfigIdIsValid(
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

        private ActionRequestValidationException checkNoIncludedAnalyzedFieldsAreExcludedBySourceFiltering(
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
            Request request = (Request) o;
            return Objects.equals(config, request.config);
        }

        @Override
        public int hashCode() {
            return Objects.hash(config);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, format("explain_data_frame_analytics[%s]", config.getId()), parentTaskId, headers);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        public static final ParseField TYPE = new ParseField("explain_data_frame_analytics_response");

        public static final ParseField FIELD_SELECTION = new ParseField("field_selection");
        public static final ParseField MEMORY_ESTIMATION = new ParseField("memory_estimation");

        @SuppressWarnings({ "unchecked" })
        static final ConstructingObjectParser<Response, Void> PARSER = new ConstructingObjectParser<>(
            TYPE.getPreferredName(),
            args -> new Response((List<FieldSelection>) args[0], (MemoryEstimation) args[1])
        );

        static {
            PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), FieldSelection.PARSER, FIELD_SELECTION);
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), MemoryEstimation.PARSER, MEMORY_ESTIMATION);
        }

        private final List<FieldSelection> fieldSelection;
        private final MemoryEstimation memoryEstimation;

        public Response(List<FieldSelection> fieldSelection, MemoryEstimation memoryEstimation) {
            this.fieldSelection = Objects.requireNonNull(fieldSelection);
            this.memoryEstimation = Objects.requireNonNull(memoryEstimation);
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            this.fieldSelection = in.readList(FieldSelection::new);
            this.memoryEstimation = new MemoryEstimation(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(fieldSelection);
            memoryEstimation.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(FIELD_SELECTION.getPreferredName(), fieldSelection);
            builder.field(MEMORY_ESTIMATION.getPreferredName(), memoryEstimation);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (other == null || getClass() != other.getClass()) return false;

            Response that = (Response) other;
            return Objects.equals(fieldSelection, that.fieldSelection) && Objects.equals(memoryEstimation, that.memoryEstimation);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldSelection, memoryEstimation);
        }

        public MemoryEstimation getMemoryEstimation() {
            return memoryEstimation;
        }

        public List<FieldSelection> getFieldSelection() {
            return fieldSelection;
        }
    }
}
