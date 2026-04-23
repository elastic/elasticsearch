/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.ccm;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.AckedBatchedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.SimpleBatchedAckListenerTaskExecutor;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.inference.InferenceFeatures;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMFeature.CCM_UNSUPPORTED_UNTIL_UPGRADED_EXCEPTION;

/**
 * This services handles managing state to determine whether the user has provided the
 * configuration to enable CCM.
 *
 * Internally it stores a flag in cluster state.
 *
 * This does not handle storing the actual CCM configuration, that is handled by {@link CCMPersistentStorageService}.
 */
public class CCMEnablementService {

    private static final String TASK_QUEUE_NAME = "inference-ccm-enabled-management";
    private static final TransportVersion INFERENCE_CCM_ENABLEMENT_SERVICE = TransportVersion.fromName("inference_ccm_enablement_service");

    private final MasterServiceTaskQueue<MetadataTask> taskQueue;
    private final FeatureService featureService;
    private final ClusterService clusterService;
    private final CCMFeature ccmFeature;

    public CCMEnablementService(ClusterService clusterService, FeatureService featureService, CCMFeature ccmFeature) {
        this.clusterService = Objects.requireNonNull(clusterService);
        this.featureService = Objects.requireNonNull(featureService);
        this.taskQueue = clusterService.createTaskQueue(TASK_QUEUE_NAME, Priority.NORMAL, new UpdateTaskExecutor());
        this.ccmFeature = Objects.requireNonNull(ccmFeature);
    }

    public boolean isEnabled(ProjectId projectId) {
        if (ccmFeature.isCcmSupportedEnvironment() == false || isClusterStateReady() == false) {
            return false;
        }

        var projectMetadata = clusterService.state().metadata().getProject(projectId);
        var metadata = EnablementMetadata.fromMetadata(projectMetadata);
        return metadata.enabled;
    }

    private boolean isClusterStateReady() {
        return clusterService.state() != null
            && clusterService.state().clusterRecovered()
            && featureService.clusterHasFeature(clusterService.state(), InferenceFeatures.INFERENCE_CCM_ENABLEMENT_SERVICE);
    }

    /**
     * This should only be called on the master node.
     * Sets the enabled state for CCM in cluster state.
     */
    public void setEnabled(ProjectId projectId, boolean enabled, ActionListener<AcknowledgedResponse> listener) {
        if (ccmFeature.isCcmSupportedEnvironment() == false) {
            listener.onFailure(CCMFeature.CCM_FORBIDDEN_EXCEPTION);
            return;
        }

        if (isClusterStateReady() == false) {
            listener.onFailure(CCM_UNSUPPORTED_UNTIL_UPGRADED_EXCEPTION);
            return;
        }

        var clusterStateListener = listener.delegateResponse((delegate, e) -> {
            delegate.onFailure(
                new ElasticsearchStatusException(
                    Strings.format("Failed to set Cloud Connected Mode cluster state to enabled: [%s]", enabled),
                    RestStatus.INTERNAL_SERVER_ERROR,
                    e
                )
            );
        });

        try {
            taskQueue.submitTask(
                "setting CCM enabled: [" + enabled + "]",
                new MetadataTask(projectId, enabled, clusterStateListener),
                null
            );
        } catch (Exception e) {
            clusterStateListener.onFailure(e);
        }
    }

    public static List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        return List.of(
            new NamedXContentRegistry.Entry(
                Metadata.ProjectCustom.class,
                new ParseField(EnablementMetadata.NAME),
                EnablementMetadata::fromXContent
            )
        );
    }

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(Metadata.ProjectCustom.class, EnablementMetadata.NAME, EnablementMetadata::new),
            new NamedWriteableRegistry.Entry(
                NamedDiff.class,
                EnablementMetadata.NAME,
                in -> AbstractNamedDiffable.readDiffFrom(Metadata.ProjectCustom.class, EnablementMetadata.NAME, in)
            )
        );
    }

    public static class EnablementMetadata extends AbstractNamedDiffable<Metadata.ProjectCustom> implements Metadata.ProjectCustom {
        public static final String NAME = "inference-ccm-enablement-management-metadata";
        private static final EnablementMetadata DISABLED = new EnablementMetadata(false);
        private static final EnablementMetadata ENABLED = new EnablementMetadata(true);
        private static final ParseField ENABLED_FIELD = new ParseField("enabled");

        private static final ConstructingObjectParser<EnablementMetadata, Void> PARSER = new ConstructingObjectParser<>(
            NAME,
            true,
            args -> new EnablementMetadata((boolean) args[0])
        );

        static {
            PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), ENABLED_FIELD);
        }

        public static EnablementMetadata fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        public static EnablementMetadata fromMetadata(ProjectMetadata projectMetadata) {
            EnablementMetadata metadata = projectMetadata.custom(NAME);
            return metadata == null ? DISABLED : metadata;
        }

        private final boolean enabled;

        // Default for testing
        EnablementMetadata(boolean enabled) {
            this.enabled = enabled;
        }

        public EnablementMetadata(StreamInput in) throws IOException {
            this(in.readBoolean());
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return Metadata.ALL_CONTEXTS;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return INFERENCE_CCM_ENABLEMENT_SERVICE;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(enabled);
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
            return Iterators.single(((builder, params) -> builder.field(ENABLED_FIELD.getPreferredName(), enabled)));
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            EnablementMetadata that = (EnablementMetadata) o;
            return enabled == that.enabled;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(enabled);
        }
    }

    private static class MetadataTask extends AckedBatchedClusterStateUpdateTask {
        private final ProjectId projectId;
        private final boolean enabled;

        private MetadataTask(ProjectId projectId, boolean enabled, ActionListener<AcknowledgedResponse> listener) {
            super(TimeValue.THIRTY_SECONDS, listener);
            this.projectId = projectId;
            this.enabled = enabled;
        }

        public ProjectId getProjectId() {
            return projectId;
        }

        public EnablementMetadata executeTask() {
            return enabled ? EnablementMetadata.ENABLED : EnablementMetadata.DISABLED;
        }
    }

    private static class UpdateTaskExecutor extends SimpleBatchedAckListenerTaskExecutor<MetadataTask> {
        @Override
        public Tuple<ClusterState, ClusterStateAckListener> executeTask(MetadataTask task, ClusterState clusterState) {
            var projectMetadata = clusterState.metadata().getProject(task.getProjectId());
            var updatedMetadata = task.executeTask();
            var newProjectMetadata = ProjectMetadata.builder(projectMetadata).putCustom(EnablementMetadata.NAME, updatedMetadata);
            return new Tuple<>(ClusterState.builder(clusterState).putProjectMetadata(newProjectMetadata).build(), task);
        }
    }
}
