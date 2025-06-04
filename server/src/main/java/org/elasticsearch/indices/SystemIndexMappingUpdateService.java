/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.SystemIndexMetadataUpgradeService;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.xcontent.XContentType;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_FORMAT_SETTING;

/**
 * This class ensures that all system indices have up-to-date mappings, provided
 * those indices can be automatically managed. Only some system indices are managed
 * internally to Elasticsearch - others are created and managed externally, e.g.
 * Kibana indices.
 *
 * Other metadata updates are handled by {@link SystemIndexMetadataUpgradeService}.
 */
public class SystemIndexMappingUpdateService implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(SystemIndexMappingUpdateService.class);

    public static final Set<String> MANAGED_SYSTEM_INDEX_SETTING_UPDATE_ALLOWLIST;
    static {
        Set<String> allowlist = new HashSet<>();
        // Add all the blocks, we need to be able to clear those
        for (IndexMetadata.APIBlock blockType : IndexMetadata.APIBlock.values()) {
            allowlist.add(blockType.settingName());
        }
        MANAGED_SYSTEM_INDEX_SETTING_UPDATE_ALLOWLIST = Collections.unmodifiableSet(allowlist);
    }

    private final SystemIndices systemIndices;
    private final Client client;
    private final ProjectResolver projectResolver;
    private final AtomicBoolean isUpgradeInProgress;

    /**
     * Creates a new manager
     * @param systemIndices the indices to manage
     * @param client used to update the cluster
     */
    public SystemIndexMappingUpdateService(SystemIndices systemIndices, Client client, ProjectResolver projectResolver) {
        this.systemIndices = systemIndices;
        this.client = client;
        this.projectResolver = projectResolver;
        this.isUpgradeInProgress = new AtomicBoolean(false);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        final ClusterState state = event.state();
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait until the gateway has recovered from disk, otherwise we may think we don't have some
            // indices but they may not have been restored from the cluster state on disk
            logger.debug("Waiting until state has been recovered");
            return;
        }

        // If this node is not a master node, exit.
        if (state.nodes().isLocalNodeElectedMaster() == false) {
            return;
        }

        // if we're in a mixed-version cluster, exit
        if (state.nodes().isMixedVersionCluster()) {
            logger.debug("Skipping system indices up-to-date check as cluster has mixed versions");
            return;
        }

        if (isUpgradeInProgress.compareAndSet(false, true)) {
            // Use a RefCountingRunnable so that we only release the lock once all upgrade attempts have succeeded or failed.
            // The failures are logged in upgradeIndexMetadata(), so we don't actually care about them here.
            try (var refs = new RefCountingRunnable(() -> isUpgradeInProgress.set(false))) {
                state.forEachProject(project -> {
                    for (SystemIndexDescriptor systemIndexDescriptor : getEligibleDescriptors(project.metadata())) {
                        UpgradeStatus upgradeStatus;
                        try {
                            upgradeStatus = getUpgradeStatus(project, systemIndexDescriptor);
                        } catch (Exception e) {
                            logger.warn(
                                Strings.format(
                                    "Failed to calculate upgrade status for project [%s]: %s",
                                    project.projectId(),
                                    e.getMessage()
                                ),
                                e
                            );
                            continue;
                        }
                        if (upgradeStatus == UpgradeStatus.NEEDS_MAPPINGS_UPDATE) {
                            upgradeIndexMappings(project.projectId(), systemIndexDescriptor, ActionListener.releasing(refs.acquire()));
                        }
                    }
                });
            }
        } else {
            logger.trace("Update already in progress");
        }
    }

    /**
     * Checks all known system index descriptors, looking for those that correspond to
     * indices that can be automatically managed and that have already been created.
     * @param projectMetadata the project metadata to consult
     * @return a list of descriptors that could potentially be updated
     */
    List<SystemIndexDescriptor> getEligibleDescriptors(ProjectMetadata projectMetadata) {
        return this.systemIndices.getSystemIndexDescriptors()
            .stream()
            .filter(SystemIndexDescriptor::isAutomaticallyManaged)
            .filter(d -> projectMetadata.hasIndexAbstraction(d.getPrimaryIndex()))
            .toList();
    }

    enum UpgradeStatus {
        CLOSED,
        UNHEALTHY,
        NEEDS_UPGRADE,
        UP_TO_DATE,
        NEEDS_MAPPINGS_UPDATE
    }

    /**
     * Determines an index's current state, with respect to whether its mappings can
     * be updated.
     *
     * @param projectState the project state to use when calculating the upgrade state
     * @param descriptor information about the system index to check
     * @return a value that indicates the index's state.
     */
    static UpgradeStatus getUpgradeStatus(ProjectState projectState, SystemIndexDescriptor descriptor) {
        final State indexState = calculateIndexState(projectState, descriptor);

        final String indexDescription = "[" + descriptor.getPrimaryIndex() + "] (alias [" + descriptor.getAliasName() + "])";

        // The messages below will be logged on every cluster state update, which is why even in the index closed / red
        // cases, the log levels are DEBUG.

        if (indexState == null) {
            logger.debug("Index {} does not exist yet", indexDescription);
            return UpgradeStatus.UP_TO_DATE;
        }

        if (indexState.indexState == IndexMetadata.State.CLOSE) {
            logger.debug("Index {} is closed. This is likely to prevent some features from functioning correctly", indexDescription);
            return UpgradeStatus.CLOSED;
        }

        if (indexState.indexHealth == ClusterHealthStatus.RED) {
            logger.debug("Index {} health status is RED, any pending mapping upgrades will wait until this changes", indexDescription);
            return UpgradeStatus.UNHEALTHY;
        }

        if (indexState.isIndexUpToDate == false) {
            logger.debug(
                "Index {} is not on the current version. Features relying "
                    + "on the index will not be available until the index is upgraded",
                indexDescription
            );
            return UpgradeStatus.NEEDS_UPGRADE;
        } else if (indexState.mappingUpToDate) {
            logger.trace("Index {} is up-to-date, no action required", indexDescription);
            return UpgradeStatus.UP_TO_DATE;
        } else {
            logger.info("Index {} mappings are not up-to-date and will be updated", indexDescription);
            return UpgradeStatus.NEEDS_MAPPINGS_UPDATE;
        }
    }

    /**
     * Updates the mappings for a system index
     * @param descriptor information about the system index
     * @param listener a listener to call upon success or failure
     */
    private void upgradeIndexMappings(
        ProjectId projectId,
        SystemIndexDescriptor descriptor,
        ActionListener<AcknowledgedResponse> listener
    ) {
        projectResolver.executeOnProject(projectId, () -> {
            final String indexName = descriptor.getPrimaryIndex();

            PutMappingRequest request = new PutMappingRequest(indexName).source(descriptor.getMappings(), XContentType.JSON);

            final OriginSettingClient originSettingClient = new OriginSettingClient(this.client, descriptor.getOrigin());

            originSettingClient.admin().indices().putMapping(request, new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse response) {
                    if (response.isAcknowledged() == false) {
                        String message = "Put mapping request for [" + indexName + "] was not acknowledged";
                        logger.error(message);
                        listener.onFailure(new ElasticsearchException(message));
                    } else {
                        listener.onResponse(response);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("Put mapping request for [" + indexName + "] failed", e);
                    listener.onFailure(e);
                }
            });
        });
    }

    /**
     * Derives a summary of the current state of a system index, relative to the given cluster state.
     * @param state the cluster state from which to derive the index state
     * @param descriptor the system index to check
     * @return a summary of the index state, or <code>null</code> if the index doesn't exist
     */
    static State calculateIndexState(ProjectState state, SystemIndexDescriptor descriptor) {
        final IndexMetadata indexMetadata = state.metadata().index(descriptor.getPrimaryIndex());

        if (indexMetadata == null) {
            return null;
        }

        final boolean isIndexUpToDate = INDEX_FORMAT_SETTING.get(indexMetadata.getSettings()) == descriptor.getIndexFormat();

        final boolean isMappingIsUpToDate = checkIndexMappingUpToDate(descriptor, indexMetadata);
        final String concreteIndexName = indexMetadata.getIndex().getName();

        final ClusterHealthStatus indexHealth;
        final IndexMetadata.State indexState = indexMetadata.getState();

        if (indexState == IndexMetadata.State.CLOSE) {
            indexHealth = null;
            logger.warn(
                "Index [{}] (alias [{}]) is closed. This is likely to prevent some features from functioning correctly",
                concreteIndexName,
                descriptor.getAliasName()
            );
        } else {
            final IndexRoutingTable routingTable = state.routingTable().index(indexMetadata.getIndex());
            indexHealth = new ClusterIndexHealth(indexMetadata, routingTable).getStatus();
        }

        return new State(indexState, indexHealth, isIndexUpToDate, isMappingIsUpToDate);
    }

    /**
     * Checks whether an index's mappings are up-to-date. If an index is encountered that has
     * a version higher than Version.CURRENT, it is still considered up-to-date.
     */
    private static boolean checkIndexMappingUpToDate(SystemIndexDescriptor descriptor, IndexMetadata indexMetadata) {
        final MappingMetadata mappingMetadata = indexMetadata.mapping();
        if (mappingMetadata == null) {
            return false;
        }

        return descriptor.getMappingsVersion().version() <= readMappingVersion(descriptor, mappingMetadata);
    }

    /**
     * Fetches the mapping version from an index's mapping's `_meta` info.
     */
    private static int readMappingVersion(SystemIndexDescriptor descriptor, MappingMetadata mappingMetadata) {
        final String indexName = descriptor.getPrimaryIndex();
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> meta = (Map<String, Object>) mappingMetadata.sourceAsMap().get("_meta");
            if (meta == null) {
                logger.warn(
                    "Missing _meta field in mapping [{}] of index [{}], assuming mappings update required",
                    mappingMetadata.type(),
                    indexName
                );
                // This can happen with old system indices, such as .watches, which were created before we had the convention of
                // storing a version under `_meta.` We should just replace the template to be sure.
                return -1;
            }

            final Object rawVersion = meta.get(SystemIndexDescriptor.VERSION_META_KEY);
            if (rawVersion == null) {
                logger.warn(
                    "No value found in mappings for [_meta.{}], assuming mappings update required",
                    SystemIndexDescriptor.VERSION_META_KEY
                );
                return -1;
            }
            if (rawVersion instanceof Integer == false) {
                logger.warn(
                    "Value in [_meta.{}] was not an integer, assuming mappings update required",
                    SystemIndexDescriptor.VERSION_META_KEY
                );
                return -1;
            }
            return (int) rawVersion;
        } catch (ElasticsearchParseException | IllegalArgumentException e) {
            logger.error(() -> "Cannot parse the mapping for index [" + indexName + "]", e);
            return -1;
        }
    }

    static class State {
        final IndexMetadata.State indexState;
        final ClusterHealthStatus indexHealth;
        final boolean isIndexUpToDate;
        final boolean mappingUpToDate;

        State(IndexMetadata.State indexState, ClusterHealthStatus indexHealth, boolean isIndexUpToDate, boolean mappingUpToDate) {
            this.indexState = indexState;
            this.indexHealth = indexHealth;
            this.isIndexUpToDate = isIndexUpToDate;
            this.mappingUpToDate = mappingUpToDate;
        }
    }
}
