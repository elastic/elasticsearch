/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.settings.put;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataUpdateSettingsService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.index.IndexModule.INDEX_RECOVERY_TYPE_SETTING;
import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;

public class TransportUpdateSettingsAction extends AcknowledgedTransportMasterNodeAction<UpdateSettingsRequest> {

    private static final Logger logger = LogManager.getLogger(TransportUpdateSettingsAction.class);

    private final MetadataUpdateSettingsService updateSettingsService;
    private final SystemIndices systemIndices;

    public static final String SNAPSHOT_DIRECTORY_FACTORY_KEY = "snapshot";

    @Inject
    public TransportUpdateSettingsAction(TransportService transportService, ClusterService clusterService,
                                         ThreadPool threadPool, MetadataUpdateSettingsService updateSettingsService,
                                         ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                         SystemIndices systemIndices) {
        super(UpdateSettingsAction.NAME, transportService, clusterService, threadPool, actionFilters, UpdateSettingsRequest::new,
            indexNameExpressionResolver, ThreadPool.Names.SAME);
        this.updateSettingsService = updateSettingsService;
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
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE,
            indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected void masterOperation(Task task, final UpdateSettingsRequest request, final ClusterState state,
                                   final ActionListener<AcknowledgedResponse> listener) {
        final Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(state, request);
        final Settings requestSettings = request.settings();


        final Map<String, List<String>> systemIndexViolations = checkForSystemIndexViolations(concreteIndices, requestSettings);
        if (systemIndexViolations.isEmpty() == false) {
            final String message = "Cannot override settings on system indices: "
                + systemIndexViolations.entrySet()
                    .stream()
                    .map(entry -> "[" + entry.getKey() + "] -> " + entry.getValue())
                    .collect(Collectors.joining(", "));
            logger.warn(message);
            listener.onFailure(new IllegalArgumentException(message));
            return;
        }

        final Map<String, List<String>> searchableSnapshotsIndexViolations =
            checkForSearchableSnapshotsIndexViolations(concreteIndices, state, requestSettings);
        if (searchableSnapshotsIndexViolations.isEmpty() == false) {
            final String message = "Cannot override settings on searchable snapshots indices: "
                + searchableSnapshotsIndexViolations.entrySet()
                .stream()
                .map(entry -> "[" + entry.getKey() + "] -> " + entry.getValue())
                .collect(Collectors.joining(", "));
            logger.warn(message);
            listener.onFailure(new IllegalArgumentException(message));
            return;
        }

        UpdateSettingsClusterStateUpdateRequest clusterStateUpdateRequest = new UpdateSettingsClusterStateUpdateRequest()
                .indices(concreteIndices)
                .settings(requestSettings)
                .setPreserveExisting(request.isPreserveExisting())
                .ackTimeout(request.timeout())
                .masterNodeTimeout(request.masterNodeTimeout());

        updateSettingsService.updateSettings(clusterStateUpdateRequest, new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse response) {
                listener.onResponse(response);
            }

            @Override
            public void onFailure(Exception t) {
                logger.debug(() -> new ParameterizedMessage("failed to update settings on indices [{}]", (Object) concreteIndices), t);
                listener.onFailure(t);
            }
        });
    }

    /**
     * Checks that if the request is trying to apply settings changes to any system indices, then the settings' values match those
     * that the system index's descriptor expects.
     *
     * @param concreteIndices the indices being updated
     * @param requestSettings the settings to be applied
     * @return a mapping from system index pattern to the settings whose values would be overridden. Empty if there are no violations.
     */
    private Map<String, List<String>> checkForSystemIndexViolations(Index[] concreteIndices, Settings requestSettings) {
        final Map<String, List<String>> violations = new HashMap<>();

        for (Index index : concreteIndices) {
            final SystemIndexDescriptor descriptor = systemIndices.findMatchingDescriptor(index.getName());
            if (descriptor != null && descriptor.isAutomaticallyManaged()) {
                final Settings descriptorSettings = descriptor.getSettings();
                List<String> failedKeys = new ArrayList<>();
                for (String key : requestSettings.keySet()) {
                    final String expectedValue = descriptorSettings.get(key);
                    final String actualValue = requestSettings.get(key);

                    if (expectedValue.equals(actualValue) == false) {
                        failedKeys.add(key);
                    }
                }

                if (failedKeys.isEmpty() == false) {
                    violations.put(descriptor.getIndexPattern(), failedKeys);
                }
            }
        }
        return violations;
    }

    /**
     * Checks that if the request is trying to apply settings changes to any searchable snapshots indices, some settings of these indices
     * cannot be updated.
     *
     * @param concreteIndices the indices being updated
     * @param state Cluster state, used to get the settings of the indices
     * @param requestSettings the settings to be applied
     * @return a mapping from searchable snapshots index to the settings whose values cannot be changed. Empty if there are no violations.
     */
    private Map<String, List<String>> checkForSearchableSnapshotsIndexViolations(Index[] concreteIndices,
                                                                                 ClusterState state, Settings requestSettings) {
        final Map<String, List<String>> violations = new HashMap<>();

        List<String> cannotUpdateSettings = Arrays.asList(IndexMetadata.SETTING_BLOCKS_WRITE,
            INDEX_STORE_TYPE_SETTING.getKey(),
            INDEX_RECOVERY_TYPE_SETTING.getKey());

        Set<String> requestSettingsKeySet = requestSettings.keySet();
        for (Index index : concreteIndices) {
            IndexMetadata indexMetadata = state.metadata().getIndexSafe(index);
            List<String> failedKeys = new ArrayList<>();
            if (SNAPSHOT_DIRECTORY_FACTORY_KEY.equals(INDEX_STORE_TYPE_SETTING.get(indexMetadata.getSettings()))) {
                for (String setting : cannotUpdateSettings) {
                    String newSettingValue = requestSettings.get(setting);
                    String oldSettingValue = indexMetadata.getSettings().get(setting);
                    if (requestSettingsKeySet.contains(setting) &&
                        (newSettingValue == null || newSettingValue.equals(oldSettingValue) == false)) {
                        failedKeys.add(setting);
                    }
                }
                if (failedKeys.isEmpty() == false) {
                    violations.put(index.getName(), failedKeys);
                }
            }
        }
        return violations;
    }
}
