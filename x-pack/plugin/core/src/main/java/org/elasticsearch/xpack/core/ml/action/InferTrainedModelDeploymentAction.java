/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelPrefixStrings;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.EmptyConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.core.Strings.format;

public class InferTrainedModelDeploymentAction extends ActionType<InferTrainedModelDeploymentAction.Response> {

    public static final InferTrainedModelDeploymentAction INSTANCE = new InferTrainedModelDeploymentAction();

    /**
     * Do not call this action directly, use InferModelAction instead
     * which will perform various checks and set the node the request
     * should execute on.
     *
     * The action is poorly named as once it was publicly accessible
     * and exposed through a REST API now it _must_ only called internally.
     */
    public static final String NAME = "cluster:monitor/xpack/ml/trained_models/deployment/infer";

    public InferTrainedModelDeploymentAction() {
        super(NAME);
    }

    /**
     * Request for inference against the deployment.
     *
     * The task gets routed to a node that indicates its local model assignment is started
     *
     * For indicating timeout, the caller should call `setInferenceTimeout` and not the base class `setTimeout` method
     */
    public static class Request extends BaseTasksRequest<Request> {

        public static final ParseField DOCS = new ParseField("docs");
        public static final ParseField TIMEOUT = new ParseField("timeout");

        public static final TimeValue DEFAULT_TIMEOUT = TimeValue.timeValueSeconds(10);

        private String id;
        private final List<Map<String, Object>> docs;
        private final InferenceConfigUpdate update;
        private final TimeValue inferenceTimeout;
        private boolean highPriority;
        // textInput added for uses that accept a query string
        // and do know which field the model expects to find its
        // input and so cannot construct a document.
        private final List<String> textInput;
        private TrainedModelPrefixStrings.PrefixType prefixType = TrainedModelPrefixStrings.PrefixType.NONE;
        private boolean chunkResults = false;

        public static Request forDocs(String id, InferenceConfigUpdate update, List<Map<String, Object>> docs, TimeValue inferenceTimeout) {
            return new Request(
                ExceptionsHelper.requireNonNull(id, InferModelAction.Request.DEPLOYMENT_ID),
                update,
                ExceptionsHelper.requireNonNull(Collections.unmodifiableList(docs), DOCS),
                null,
                inferenceTimeout
            );
        }

        public static Request forTextInput(String id, InferenceConfigUpdate update, List<String> textInput, TimeValue inferenceTimeout) {
            return new Request(
                ExceptionsHelper.requireNonNull(id, InferModelAction.Request.DEPLOYMENT_ID),
                update,
                List.of(),
                ExceptionsHelper.requireNonNull(textInput, "inference text input"),
                inferenceTimeout
            );
        }

        // for tests
        Request(
            String id,
            InferenceConfigUpdate update,
            List<Map<String, Object>> docs,
            List<String> textInput,
            TimeValue inferenceTimeout
        ) {
            this.id = ExceptionsHelper.requireNonNull(id, InferModelAction.Request.DEPLOYMENT_ID);
            this.docs = docs;
            this.textInput = textInput;
            this.update = update;
            this.inferenceTimeout = inferenceTimeout;
            this.highPriority = false;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            id = in.readString();
            docs = in.readCollectionAsImmutableList(StreamInput::readGenericMap);
            update = in.readOptionalNamedWriteable(InferenceConfigUpdate.class);
            inferenceTimeout = in.readOptionalTimeValue();
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_3_0)) {
                highPriority = in.readBoolean();
            }
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_7_0)) {
                textInput = in.readOptionalStringCollectionAsList();
            } else {
                textInput = null;
            }
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
                prefixType = in.readEnum(TrainedModelPrefixStrings.PrefixType.class);
            } else {
                prefixType = TrainedModelPrefixStrings.PrefixType.NONE;
            }
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
                chunkResults = in.readBoolean();
            } else {
                chunkResults = false;
            }
        }

        public String getId() {
            return id;
        }

        public List<Map<String, Object>> getDocs() {
            return docs;
        }

        public List<String> getTextInput() {
            return textInput;
        }

        public InferenceConfigUpdate getUpdate() {
            return Optional.ofNullable(update).orElse(new EmptyConfigUpdate());
        }

        public TimeValue getInferenceTimeout() {
            return inferenceTimeout == null ? DEFAULT_TIMEOUT : inferenceTimeout;
        }

        public void setId(String id) {
            this.id = id;
        }

        /**
         * This is always null as we want the inference call to handle the timeout, not the tasks framework
         * @return null
         */
        @Override
        @Nullable
        public TimeValue getTimeout() {
            return null;
        }

        public void setHighPriority(boolean highPriority) {
            this.highPriority = highPriority;
        }

        public boolean isHighPriority() {
            return highPriority;
        }

        public void setPrefixType(TrainedModelPrefixStrings.PrefixType prefixType) {
            this.prefixType = prefixType;
        }

        public TrainedModelPrefixStrings.PrefixType getPrefixType() {
            return prefixType;
        }

        public boolean isChunkResults() {
            return chunkResults;
        }

        public void setChunkResults(boolean chunkResults) {
            this.chunkResults = chunkResults;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = super.validate();
            if (docs == null) {
                validationException = addValidationError("[" + DOCS.getPreferredName() + "] must not be null", validationException);
            } else {
                if (docs.isEmpty() && textInput == null) {
                    validationException = addValidationError("at least one document is required ", validationException);
                }
            }
            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(id);
            out.writeCollection(docs, StreamOutput::writeGenericMap);
            out.writeOptionalNamedWriteable(update);
            out.writeOptionalTimeValue(inferenceTimeout);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_3_0)) {
                out.writeBoolean(highPriority);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_7_0)) {
                out.writeOptionalStringCollection(textInput);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
                out.writeEnum(prefixType);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
                out.writeBoolean(chunkResults);
            }
        }

        @Override
        public boolean match(Task task) {
            return StartTrainedModelDeploymentAction.TaskMatcher.match(task, id);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            InferTrainedModelDeploymentAction.Request that = (InferTrainedModelDeploymentAction.Request) o;
            return Objects.equals(id, that.id)
                && Objects.equals(docs, that.docs)
                && Objects.equals(update, that.update)
                && Objects.equals(inferenceTimeout, that.inferenceTimeout)
                && Objects.equals(highPriority, that.highPriority)
                && Objects.equals(textInput, that.textInput)
                && (prefixType == that.prefixType)
                && (chunkResults == that.chunkResults);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, update, docs, inferenceTimeout, highPriority, textInput, prefixType, chunkResults);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, format("infer_trained_model_deployment[%s]", this.id), parentTaskId, headers);
        }

    }

    public static class Response extends BaseTasksResponse implements Writeable, ToXContentObject {

        private final List<InferenceResults> results;

        public Response(List<InferenceResults> results) {
            super(Collections.emptyList(), Collections.emptyList());
            this.results = Objects.requireNonNull(results);
        }

        public Response(StreamInput in) throws IOException {
            super(in);

            // Multiple results added in 8.6.1
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_6_1)) {
                results = in.readNamedWriteableCollectionAsList(InferenceResults.class);
            } else {
                results = List.of(in.readNamedWriteable(InferenceResults.class));
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);

            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_6_1)) {
                out.writeNamedWriteableCollection(results);
            } else {
                out.writeNamedWriteable(results.get(0));
            }
        }

        public List<InferenceResults> getResults() {
            return results;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            results.get(0).toXContent(builder, params);
            builder.endObject();
            return builder;
        }
    }
}
