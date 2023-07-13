/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.settings;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsClusterStateUpdateRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataUpdateSettingsService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.settings.UpdateSecuritySettingsAction;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.security.action.settings.TransportGetSecuritySettingsAction.resolveConcreteIndex;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_PROFILE_ALIAS;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_TOKENS_ALIAS;

public class TransportUpdateSecuritySettingsAction extends TransportMasterNodeAction<
    UpdateSecuritySettingsAction.Request,
    AcknowledgedResponse> {

    private final MetadataUpdateSettingsService updateSettingsService;

    @Inject
    public TransportUpdateSecuritySettingsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        MetadataUpdateSettingsService metadataUpdateSettingsService,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            UpdateSecuritySettingsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            UpdateSecuritySettingsAction.Request::new,
            indexNameExpressionResolver,
            AcknowledgedResponse::readFrom,
            ThreadPool.Names.SAME
        );
        this.updateSettingsService = metadataUpdateSettingsService;
    }

    @Override
    protected void masterOperation(
        Task task,
        UpdateSecuritySettingsAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) throws Exception {

        List<UpdateSettingsClusterStateUpdateRequest> settingsUpdateRequests = Stream.of(
            updateSettingsForIndex(
                SECURITY_MAIN_ALIAS,
                Settings.builder().loadFromMap(request.mainIndexSettings()).build(),
                request.timeout(),
                request.masterNodeTimeout(),
                state
            ),
            updateSettingsForIndex(
                SECURITY_TOKENS_ALIAS,
                Settings.builder().loadFromMap(request.tokensIndexSettings()).build(),
                request.timeout(),
                request.masterNodeTimeout(),
                state
            ),
            updateSettingsForIndex(
                SECURITY_PROFILE_ALIAS,
                Settings.builder().loadFromMap(request.profilesIndexSettings()).build(),
                request.timeout(),
                request.masterNodeTimeout(),
                state
            )
        ).filter(Optional::isPresent).map(Optional::get).toList();
        if (settingsUpdateRequests.isEmpty() == false) {
            ActionListener<AcknowledgedResponse> groupedListener = new GroupedActionListener<>(
                settingsUpdateRequests.size(),
                ActionListener.wrap((responses) -> {
                    listener.onResponse(AcknowledgedResponse.of(responses.stream().allMatch(AcknowledgedResponse::isAcknowledged)));
                }, listener::onFailure)
            );
            settingsUpdateRequests.forEach(req -> updateSettingsService.updateSettings(req, groupedListener));
        } else {
            // All settings blocks were empty, so... yay, we did it
            listener.onResponse(AcknowledgedResponse.TRUE);
        }
    }

    private Optional<UpdateSettingsClusterStateUpdateRequest> updateSettingsForIndex(
        String indexName,
        Settings settingsToUpdate,
        TimeValue timeout,
        TimeValue masterTimeout,
        ClusterState state
    ) {
        IndexAbstraction abstraction = state.metadata().getIndicesLookup().get(indexName);
        if (abstraction == null) {
            if (settingsToUpdate.isEmpty() == false) {
                throw new ResourceNotFoundException(indexName);
            } else {
                return Optional.empty();
            }
        }
        Index writeIndex = abstraction.getWriteIndex();
        if (writeIndex == null) {
            throw new IllegalStateException(Strings.format("security system alias [%s] exists but does not have a write index"));
        }

        return Optional.of(
            new UpdateSettingsClusterStateUpdateRequest().indices(new Index[] { writeIndex })
                .settings(settingsToUpdate)
                .ackTimeout(timeout)
                .masterNodeTimeout(masterTimeout)
        );
    }

    @Override
    protected ClusterBlockException checkBlock(UpdateSecuritySettingsAction.Request request, ClusterState state) {
        ClusterBlockException globalBlock = state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        if (globalBlock != null) {
            return globalBlock;
        }
        List<String> indices = new ArrayList<>(3);
        if (request.mainIndexSettings().isEmpty() == false) {
            indices.add(SECURITY_MAIN_ALIAS);
        }
        if (request.tokensIndexSettings().isEmpty() == false) {
            indices.add(SECURITY_TOKENS_ALIAS);
        }
        if (request.profilesIndexSettings().isEmpty() == false) {
            indices.add(SECURITY_PROFILE_ALIAS);
        }

        // Don't use the indexNameExpressionResolver here so we don't trigger a system index deprecation warning
        String[] concreteIndices = indices.stream()
            .map(alias -> resolveConcreteIndex(alias, state).map(Index::getName))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .toArray(String[]::new);
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, concreteIndices);
    }
}
