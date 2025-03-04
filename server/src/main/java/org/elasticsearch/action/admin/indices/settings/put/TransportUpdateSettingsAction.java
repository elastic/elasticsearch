/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.settings.put;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataUpdateSettingsService;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.indices.SystemIndexMappingUpdateService.MANAGED_SYSTEM_INDEX_SETTING_UPDATE_ALLOWLIST;

public class TransportUpdateSettingsAction extends AcknowledgedTransportMasterNodeAction<UpdateSettingsRequest> {

    public static final ActionType<AcknowledgedResponse> TYPE = new ActionType<>("indices:admin/settings/update");
    private static final Logger logger = LogManager.getLogger(TransportUpdateSettingsAction.class);

    private final MetadataUpdateSettingsService updateSettingsService;
    private final ProjectResolver projectResolver;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final SystemIndices systemIndices;

    @Inject
    public TransportUpdateSettingsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataUpdateSettingsService updateSettingsService,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SystemIndices systemIndices
    ) {
        super(
            TYPE.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            UpdateSettingsRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.updateSettingsService = updateSettingsService;
        this.projectResolver = projectResolver;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.systemIndices = systemIndices;
    }

    @Override
    protected ClusterBlockException checkBlock(UpdateSettingsRequest request, ClusterState state) {
        // allow for dedicated changes to the metadata blocks, so we don't block those to allow to "re-enable" it
        ClusterBlockException globalBlock = state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        if (globalBlock != null) {
            return globalBlock;
        }
        if (request.settings().size() == 1 &&  // we have to allow resetting these settings otherwise users can't unblock an index
            IndexMetadata.INDEX_BLOCKS_METADATA_SETTING.exists(request.settings())
            || IndexMetadata.INDEX_READ_ONLY_SETTING.exists(request.settings())
            || IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING.exists(request.settings())) {
            return null;
        }
        final ProjectMetadata projectMetadata = projectResolver.getProjectMetadata(state);
        return state.blocks()
            .indicesBlockedException(
                projectMetadata.id(),
                ClusterBlockLevel.METADATA_WRITE,
                indexNameExpressionResolver.concreteIndexNames(projectMetadata, request)
            );
    }

    @Override
    protected void masterOperation(
        Task task,
        final UpdateSettingsRequest request,
        final ClusterState state,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        final Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(state, request);
        final Settings requestSettings = request.settings();

        final Map<String, List<String>> systemIndexViolations = checkForSystemIndexViolations(concreteIndices, request);
        if (systemIndexViolations.isEmpty() == false) {
            final String message = "Cannot override settings on system indices: "
                + systemIndexViolations.entrySet()
                    .stream()
                    .map(entry -> "[" + entry.getKey() + "] -> " + entry.getValue())
                    .collect(Collectors.joining(", "));
            logger.warn(message);
            listener.onFailure(new IllegalStateException(message));
            return;
        }

        final List<String> unhiddenSystemIndexViolations = checkForUnhidingSystemIndex(concreteIndices, request);
        if (unhiddenSystemIndexViolations.isEmpty() == false) {
            final String message = "Cannot set [index.hidden] to 'false' on system indices: "
                + unhiddenSystemIndexViolations.stream().map(entry -> "[" + entry + "]").collect(Collectors.joining(", "));
            logger.warn(message);
            listener.onFailure(new IllegalStateException(message));
            return;
        }

        updateSettingsService.updateSettings(
            new UpdateSettingsClusterStateUpdateRequest(
                projectResolver.getProjectId(),
                request.masterNodeTimeout(),
                request.ackTimeout(),
                requestSettings,
                request.isPreserveExisting()
                    ? UpdateSettingsClusterStateUpdateRequest.OnExisting.PRESERVE
                    : UpdateSettingsClusterStateUpdateRequest.OnExisting.OVERWRITE,
                request.reopen()
                    ? UpdateSettingsClusterStateUpdateRequest.OnStaticSetting.REOPEN_INDICES
                    : UpdateSettingsClusterStateUpdateRequest.OnStaticSetting.REJECT,
                concreteIndices
            ),
            listener.delegateResponse((l, e) -> {
                logger.debug(() -> "failed to update settings on indices [" + Arrays.toString(concreteIndices) + "]", e);
                l.onFailure(e);
            })
        );
    }

    /**
     * Checks that if the request is trying to apply settings changes to any system indices, then the settings' values match those
     * that the system index's descriptor expects.
     *
     * @param concreteIndices the indices being updated
     * @param request the update request
     * @return a mapping from system index pattern to the settings whose values would be overridden. Empty if there are no violations.
     */
    private Map<String, List<String>> checkForSystemIndexViolations(Index[] concreteIndices, UpdateSettingsRequest request) {
        // Requests that a cluster generates itself are permitted to have a difference in settings
        // so that rolling upgrade scenarios still work. We check this via the request's origin.
        if (Strings.isNullOrEmpty(request.origin()) == false) {
            return Map.of();
        }

        final Map<String, List<String>> violationsByIndex = new HashMap<>();
        final Settings requestSettings = request.settings();

        for (Index index : concreteIndices) {
            final SystemIndexDescriptor descriptor = systemIndices.findMatchingDescriptor(index.getName());
            if (descriptor != null && descriptor.isAutomaticallyManaged()) {
                final Settings descriptorSettings = descriptor.getSettings();
                List<String> failedKeys = new ArrayList<>();
                for (String key : requestSettings.keySet()) {
                    if (MANAGED_SYSTEM_INDEX_SETTING_UPDATE_ALLOWLIST.contains(key)) {
                        // Don't check the setting if it's on the allowlist.
                        continue;
                    }
                    final String expectedValue = descriptorSettings.get(key);
                    final String actualValue = requestSettings.get(key);

                    if (Objects.equals(expectedValue, actualValue) == false) {
                        failedKeys.add(key);
                    }
                }

                if (failedKeys.isEmpty() == false) {
                    violationsByIndex.put(descriptor.getIndexPattern(), failedKeys);
                }
            }
        }

        return violationsByIndex;
    }

    /**
     * Checks that the request isn't trying to remove the "hidden" setting on a system
     * index
     *
     * @param concreteIndices the indices being updated
     * @param request the update request
     * @return a list of system indexes that this request would make visible
     */
    private List<String> checkForUnhidingSystemIndex(Index[] concreteIndices, UpdateSettingsRequest request) {
        // Requests that a cluster generates itself are permitted to have a difference in settings
        // so that rolling upgrade scenarios still work. We check this via the request's origin.
        if (request.settings().getAsBoolean(IndexMetadata.SETTING_INDEX_HIDDEN, true)) {
            return List.of();
        }

        final List<String> systemPatterns = new ArrayList<>();

        for (Index index : concreteIndices) {
            final SystemIndexDescriptor descriptor = systemIndices.findMatchingDescriptor(index.getName());
            if (descriptor != null) {
                systemPatterns.add(index.getName());
            }
        }

        return systemPatterns;
    }
}
