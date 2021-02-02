/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.gateway.GatewayService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_FORMAT_SETTING;

/**
 * This class ensures that all system indices have up-to-date mappings, provided
 * those indices can be automatically managed. Only some system indices are managed
 * internally to Elasticsearch - others are created and managed externally, e.g.
 * Kibana indices.
 */
public class SystemIndexManager implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(SystemIndexManager.class);

    private final SystemIndices systemIndices;
    private final Client client;
    private final AtomicBoolean isUpgradeInProgress;

    public SystemIndexManager(SystemIndices systemIndices, Client client) {
        this.systemIndices = systemIndices;
        this.client = client;
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

        if (state.nodes().getMaxNodeVersion().after(state.nodes().getMinNodeVersion())) {
            logger.debug("Skipping system indices up-to-date check as cluster has mixed versions");
            return;
        }

        if (isUpgradeInProgress.compareAndSet(false, true)) {
            final List<SystemIndexDescriptor> descriptors = getEligibleDescriptors(state.getMetadata()).stream()
                .filter(descriptor -> getUpgradeStatus(state, descriptor) == UpgradeStatus.NEEDS_MAPPINGS_UPDATE)
                .collect(Collectors.toList());

            if (descriptors.isEmpty() == false) {
                // Use a GroupedActionListener so that we only release the lock once all upgrade attempts have succeeded or failed.
                // The failures are logged in upgradeIndexMetadata(), so we don't actually care about them here.
                ActionListener<AcknowledgedResponse> listener = new GroupedActionListener<>(
                    ActionListener.wrap(() -> isUpgradeInProgress.set(false)),
                    descriptors.size()
                );

                descriptors.forEach(descriptor -> upgradeIndexMetadata(descriptor, listener));
            } else {
                isUpgradeInProgress.set(false);
            }
        } else {
            logger.trace("Update already in progress");
        }
    }

    /**
     * Checks all known system index descriptors, looking for those that correspond to
     * indices that can be automatically managed and that have already been created.
     * @param metadata the cluster state metadata to consult
     * @return a list of descriptors that could potentially be updated
     */
    List<SystemIndexDescriptor> getEligibleDescriptors(Metadata metadata) {
        return this.systemIndices.getSystemIndexDescriptors()
            .stream()
            .filter(SystemIndexDescriptor::isAutomaticallyManaged)
            .filter(d -> metadata.hasConcreteIndex(d.getPrimaryIndex()))
            .collect(Collectors.toList());
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
     * @param clusterState the cluster state to use when calculating the upgrade state
     * @param descriptor information about the system index to check
     * @return a value that indicates the index's state.
     */
    UpgradeStatus getUpgradeStatus(ClusterState clusterState, SystemIndexDescriptor descriptor) {
        final State indexState = calculateIndexState(clusterState, descriptor);

        final String indexDescription = "[" + descriptor.getPrimaryIndex() + "] (alias [" + descriptor.getAliasName() + "])";

        // The messages below will be logged on every cluster state update, which is why even in the index closed / red
        // cases, the log levels are DEBUG.

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
    private void upgradeIndexMetadata(SystemIndexDescriptor descriptor, ActionListener<AcknowledgedResponse> listener) {
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
    }

    /**
     * Derives a summary of the current state of a system index, relative to the given cluster state.
     */
    State calculateIndexState(ClusterState state, SystemIndexDescriptor descriptor) {
        final IndexMetadata indexMetadata = state.metadata().index(descriptor.getPrimaryIndex());
        assert indexMetadata != null;

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
            final IndexRoutingTable routingTable = state.getRoutingTable().index(indexMetadata.getIndex());
            indexHealth = new ClusterIndexHealth(indexMetadata, routingTable).getStatus();
        }

        return new State(indexState, indexHealth, isIndexUpToDate, isMappingIsUpToDate);
    }

    /**
     * Checks whether an index's mappings are up-to-date. If an index is encountered that has
     * a version higher than Version.CURRENT, it is still considered up-to-date.
     */
    private boolean checkIndexMappingUpToDate(SystemIndexDescriptor descriptor, IndexMetadata indexMetadata) {
        final MappingMetadata mappingMetadata = indexMetadata.mapping();
        if (mappingMetadata == null) {
            return false;
        }

        return Version.CURRENT.onOrBefore(readMappingVersion(descriptor, mappingMetadata));
    }

    /**
     * Fetches the mapping version from an index's mapping's `_meta` info.
     */
    @SuppressWarnings("unchecked")
    private Version readMappingVersion(SystemIndexDescriptor descriptor, MappingMetadata mappingMetadata) {
        final String indexName = descriptor.getPrimaryIndex();
        try {
            Map<String, Object> meta = (Map<String, Object>) mappingMetadata.sourceAsMap().get("_meta");
            if (meta == null) {
                logger.warn("Missing _meta field in mapping [{}] of index [{}]", mappingMetadata.type(), indexName);
                throw new IllegalStateException("Cannot read version string in index " + indexName);
            }

            final String versionString = (String) meta.get(descriptor.getVersionMetaKey());
            if (versionString == null) {
                logger.warn("No value found in mappings for [_meta.{}]", descriptor.getVersionMetaKey());
            }
            return Version.fromString(versionString);
        } catch (ElasticsearchParseException e) {
            logger.error(new ParameterizedMessage("Cannot parse the mapping for index [{}]", indexName), e);
            throw new ElasticsearchException("Cannot parse the mapping for index [{}]", e, indexName);
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
