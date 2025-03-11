/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step;
import org.elasticsearch.xpack.core.ilm.action.ILMActions;
import org.elasticsearch.xpack.ilm.IndexLifecycleService;

import java.io.IOException;
import java.util.Objects;

public class TransportMoveToStepAction extends TransportMasterNodeAction<TransportMoveToStepAction.Request, AcknowledgedResponse> {
    private static final Logger logger = LogManager.getLogger(TransportMoveToStepAction.class);

    IndexLifecycleService indexLifecycleService;

    @Inject
    public TransportMoveToStepAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexLifecycleService indexLifecycleService
    ) {
        super(
            ILMActions.MOVE_TO_STEP.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            Request::new,
            AcknowledgedResponse::readFrom,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.indexLifecycleService = indexLifecycleService;
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
        IndexMetadata indexMetadata = state.metadata().getProject().index(request.getIndex());
        if (indexMetadata == null) {
            listener.onFailure(new IllegalArgumentException("index [" + request.getIndex() + "] does not exist"));
            return;
        }

        final String policyName = indexMetadata.getLifecyclePolicyName();
        if (policyName == null) {
            listener.onFailure(
                new IllegalArgumentException("index [" + request.getIndex() + "] is not associated with an Index Lifecycle Policy")
            );
            return;
        }

        final Request.PartialStepKey abstractTargetKey = request.getNextStepKey();
        final String targetStr = abstractTargetKey.getPhase() + "/" + abstractTargetKey.getAction() + "/" + abstractTargetKey.getName();

        // Resolve the key that could have optional parts into one
        // that is totally concrete given the existing policy and index
        Step.StepKey concreteTargetStepKey = indexLifecycleService.resolveStepKey(
            state,
            indexMetadata.getIndex(),
            abstractTargetKey.getPhase(),
            abstractTargetKey.getAction(),
            abstractTargetKey.getName()
        );

        // We do a pre-check here before invoking the cluster state update just so we can skip the submission if the request is bad.
        if (concreteTargetStepKey == null) {
            // This means we weren't able to find the key they specified
            String message = "cannot move index ["
                + indexMetadata.getIndex().getName()
                + "] with policy ["
                + policyName
                + "]: unable to determine concrete step key from target next step key: "
                + abstractTargetKey;
            logger.warn(message);
            listener.onFailure(new IllegalArgumentException(message));
            return;
        }

        submitUnbatchedTask(
            "index[" + request.getIndex() + "]-move-to-step-" + targetStr,
            new AckedClusterStateUpdateTask(request, listener) {
                final SetOnce<Step.StepKey> concreteTargetKey = new SetOnce<>();

                @Override
                public ClusterState execute(ClusterState currentState) {
                    // Resolve the key that could have optional parts into one
                    // that is totally concrete given the existing policy and index
                    Step.StepKey concreteTargetStepKey = indexLifecycleService.resolveStepKey(
                        state,
                        indexMetadata.getIndex(),
                        abstractTargetKey.getPhase(),
                        abstractTargetKey.getAction(),
                        abstractTargetKey.getName()
                    );

                    // Make one more check, because it could have changed in the meantime. If that is the case, the request is ignored.
                    if (concreteTargetStepKey == null) {
                        // This means we weren't able to find the key they specified
                        logger.error(
                            "unable to move index "
                                + indexMetadata.getIndex()
                                + " as we are unable to resolve a concrete "
                                + "step key from target next step key: "
                                + abstractTargetKey
                        );
                        return currentState;
                    }

                    concreteTargetKey.set(concreteTargetStepKey);
                    return indexLifecycleService.moveClusterStateToStep(
                        currentState,
                        indexMetadata.getIndex(),
                        request.getCurrentStepKey(),
                        concreteTargetKey.get()
                    );
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    IndexMetadata newIndexMetadata = newState.metadata().getProject().index(indexMetadata.getIndex());
                    if (newIndexMetadata == null) {
                        // The index has somehow been deleted - there shouldn't be any opportunity for this to happen, but just in case.
                        logger.debug(
                            "index ["
                                + indexMetadata.getIndex()
                                + "] has been deleted after moving to step ["
                                + concreteTargetKey.get()
                                + "], skipping async action check"
                        );
                        return;
                    }
                    indexLifecycleService.maybeRunAsyncAction(newState, newIndexMetadata, concreteTargetKey.get());
                }
            }
        );
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject {

        public interface Factory {
            Request create(Step.StepKey currentStepKey, PartialStepKey nextStepKey);
        }

        static final ParseField CURRENT_KEY_FIELD = new ParseField("current_step");
        static final ParseField NEXT_KEY_FIELD = new ParseField("next_step");
        private static final ConstructingObjectParser<Request, Factory> PARSER = new ConstructingObjectParser<>(
            "move_to_step_request",
            false,
            (a, factory) -> {
                Step.StepKey currentStepKey = (Step.StepKey) a[0];
                PartialStepKey nextStepKey = (PartialStepKey) a[1];
                return factory.create(currentStepKey, nextStepKey);
            }
        );

        static {
            // The current step uses the strict parser (meaning it requires all three parts of a stepkey)
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, name) -> Step.StepKey.parse(p), CURRENT_KEY_FIELD);
            // The target step uses the parser that allows specifying only the phase, or the phase and action
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, name) -> PartialStepKey.parse(p), NEXT_KEY_FIELD);
        }

        private final String index;
        private final Step.StepKey currentStepKey;
        private final PartialStepKey nextStepKey;

        public Request(
            TimeValue masterNodeTimeout,
            TimeValue ackTimeout,
            String index,
            Step.StepKey currentStepKey,
            PartialStepKey nextStepKey
        ) {
            super(masterNodeTimeout, ackTimeout);
            this.index = index;
            this.currentStepKey = currentStepKey;
            this.nextStepKey = nextStepKey;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.index = in.readString();
            this.currentStepKey = Step.StepKey.readFrom(in);
            this.nextStepKey = new PartialStepKey(in);
        }

        public String getIndex() {
            return index;
        }

        public Step.StepKey getCurrentStepKey() {
            return currentStepKey;
        }

        public PartialStepKey getNextStepKey() {
            return nextStepKey;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public static Request parseRequest(Factory factory, XContentParser parser) {
            return PARSER.apply(parser, factory);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(index);
            currentStepKey.writeTo(out);
            nextStepKey.writeTo(out);
        }

        @Override
        public int hashCode() {
            return Objects.hash(index, currentStepKey, nextStepKey);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj.getClass() != getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(index, other.index)
                && Objects.equals(currentStepKey, other.currentStepKey)
                && Objects.equals(nextStepKey, other.nextStepKey);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                .field(CURRENT_KEY_FIELD.getPreferredName(), currentStepKey)
                .field(NEXT_KEY_FIELD.getPreferredName(), nextStepKey)
                .endObject();
        }

        /**
         * A PartialStepKey is like a {@link Step.StepKey}, however, the action and step name are optional.
         */
        public static class PartialStepKey implements Writeable, ToXContentObject {
            private final String phase;
            private final String action;
            private final String name;

            public static final ParseField PHASE_FIELD = new ParseField("phase");
            public static final ParseField ACTION_FIELD = new ParseField("action");
            public static final ParseField NAME_FIELD = new ParseField("name");
            private static final ConstructingObjectParser<PartialStepKey, Void> PARSER = new ConstructingObjectParser<>(
                "step_specification",
                a -> new PartialStepKey((String) a[0], (String) a[1], (String) a[2])
            );

            static {
                PARSER.declareString(ConstructingObjectParser.constructorArg(), PHASE_FIELD);
                PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), ACTION_FIELD);
                PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), NAME_FIELD);
            }

            public PartialStepKey(String phase, @Nullable String action, @Nullable String name) {
                this.phase = phase;
                this.action = action;
                this.name = name;
                if (name != null && action == null) {
                    throw new IllegalArgumentException(
                        "phase; phase and action; or phase, action, and step must be provided, "
                            + "but a step name was specified without a corresponding action"
                    );
                }
            }

            public PartialStepKey(StreamInput in) throws IOException {
                this.phase = in.readString();
                this.action = in.readOptionalString();
                this.name = in.readOptionalString();
                if (name != null && action == null) {
                    throw new IllegalArgumentException(
                        "phase; phase and action; or phase, action, and step must be provided, "
                            + "but a step name was specified without a corresponding action"
                    );
                }
            }

            public static PartialStepKey parse(XContentParser parser) {
                return PARSER.apply(parser, null);
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(phase);
                out.writeOptionalString(action);
                out.writeOptionalString(name);
            }

            @Nullable
            public String getPhase() {
                return phase;
            }

            @Nullable
            public String getAction() {
                return action;
            }

            @Nullable
            public String getName() {
                return name;
            }

            @Override
            public int hashCode() {
                return Objects.hash(phase, action, name);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == null) {
                    return false;
                }
                if (getClass() != obj.getClass()) {
                    return false;
                }
                PartialStepKey other = (PartialStepKey) obj;
                return Objects.equals(phase, other.phase) && Objects.equals(action, other.action) && Objects.equals(name, other.name);
            }

            @Override
            public String toString() {
                return Strings.toString(this);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field(PHASE_FIELD.getPreferredName(), phase);
                if (action != null) {
                    builder.field(ACTION_FIELD.getPreferredName(), action);
                }
                if (name != null) {
                    builder.field(NAME_FIELD.getPreferredName(), name);
                }
                builder.endObject();
                return builder;
            }
        }
    }
}
