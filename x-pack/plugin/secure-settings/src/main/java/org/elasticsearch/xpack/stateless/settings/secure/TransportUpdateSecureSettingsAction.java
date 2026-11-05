/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.settings.secure;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.settings.UpdateSecureSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.UpdateSecureSettingsRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SequentialAckingBatchedTaskExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ClusterSecrets;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.CharArrays;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Stores secure setting values set through {@code PUT _cluster/settings} in {@link SecureSettingsMetadata}, encrypted with the project
 * encryption key. There is no plain text fallback: if the value can't be encrypted, the request fails and nothing is stored.
 */
public class TransportUpdateSecureSettingsAction extends TransportMasterNodeAction<UpdateSecureSettingsRequest, AcknowledgedResponse> {

    private final ProjectResolver projectResolver;
    private final FeatureService featureService;
    private final EncryptionService encryptionService;
    private final MasterServiceTaskQueue<AckedClusterStateUpdateTask> taskQueue;

    @Inject
    public TransportUpdateSecureSettingsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
        FeatureService featureService,
        EncryptionService encryptionService
    ) {
        super(
            UpdateSecureSettingsAction.NAME,
            false,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            UpdateSecureSettingsRequest::new,
            AcknowledgedResponse::readFrom,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.projectResolver = projectResolver;
        this.featureService = featureService;
        this.encryptionService = encryptionService;
        this.taskQueue = clusterService.createTaskQueue(
            "update-secure-settings",
            Priority.NORMAL,
            new SequentialAckingBatchedTaskExecutor<>()
        );
    }

    @Override
    protected ClusterBlockException checkBlock(UpdateSecureSettingsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    /**
     * Secure settings in reserved state come from the {@code cluster_secrets} handler, not from {@code cluster_settings}.
     */
    @Override
    public Optional<String> reservedStateHandlerName() {
        return Optional.of(ClusterSecrets.NAME);
    }

    @Override
    public Set<String> modifiedKeys(UpdateSecureSettingsRequest request) {
        return request.settingNames();
    }

    @Override
    protected void masterOperation(
        Task task,
        UpdateSecureSettingsRequest request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        if (featureService.clusterHasFeature(state, UpdateSecureSettingsAction.SECURE_SETTINGS_API) == false) {
            throw new IllegalArgumentException("secure settings cannot be set until all nodes in the cluster support it");
        }
        final ProjectId projectId = projectResolver.getProjectId();
        if (ProjectId.DEFAULT.equals(projectId) == false) {
            throw new IllegalArgumentException("secure settings can only be set in the default project");
        }
        request.settings().forEach(this::validate);
        final Map<String, EncryptedData> updates = encrypt(request.settings());
        final AckedClusterStateUpdateTask updateTask = new AckedClusterStateUpdateTask(request, listener) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                final ProjectMetadata project = currentState.metadata().getProject(projectId);
                final Map<String, EncryptedData> settings = new HashMap<>(SecureSettingsMetadata.get(project).settings());
                for (Map.Entry<String, EncryptedData> update : updates.entrySet()) {
                    if (update.getValue() == null) {
                        settings.remove(update.getKey());
                    } else {
                        settings.put(update.getKey(), update.getValue());
                    }
                }
                final ProjectMetadata.Builder builder = ProjectMetadata.builder(project);
                if (settings.isEmpty()) {
                    builder.removeCustom(SecureSettingsMetadata.TYPE);
                } else {
                    builder.putCustom(SecureSettingsMetadata.TYPE, new SecureSettingsMetadata(settings));
                }
                return ClusterState.builder(currentState).putProjectMetadata(builder).build();
            }
        };
        taskQueue.submitTask("update-secure-settings " + request.settingNames(), updateTask, updateTask.timeout());
    }

    /**
     * Dependencies between settings aren't checked: they can be met by a node's keystore, which the master can't see.
     */
    private void validate(String name, SecureString value) {
        final Setting<?> setting = clusterService.getClusterSettings().get(name);
        if (setting instanceof SecureSetting<?> == false) {
            throw new IllegalArgumentException("secure setting [" + name + "] is not recognized");
        }
        if (setting.isDynamic() == false) {
            throw new IllegalArgumentException("secure setting [" + name + "] cannot be set through the API");
        }
        if (setting.isOperatorOnly()) {
            throw new IllegalArgumentException("operator-only secure setting [" + name + "] cannot be set through the API yet");
        }
        if (value != null && value.length() == 0) {
            throw new IllegalArgumentException("secure setting [" + name + "] cannot be empty; set it to null to remove it");
        }
    }

    /**
     * Encrypts every value before the cluster state task is submitted. A {@code null} value stays {@code null} and removes the setting.
     */
    private Map<String, EncryptedData> encrypt(Map<String, SecureString> values) {
        final Map<String, EncryptedData> encrypted = new HashMap<>(values.size());
        for (Map.Entry<String, SecureString> entry : values.entrySet()) {
            if (entry.getValue() == null) {
                encrypted.put(entry.getKey(), null);
                continue;
            }
            final byte[] plaintext = CharArrays.toUtf8Bytes(entry.getValue().getChars());
            try {
                encrypted.put(entry.getKey(), encryptionService.encrypt(plaintext));
            } finally {
                Arrays.fill(plaintext, (byte) 0);
            }
        }
        return encrypted;
    }
}
