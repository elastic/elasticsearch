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
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.gateway.GatewayService;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_FORMAT_SETTING;

public class SystemIndexManager implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(SystemIndexManager.class);

    private final SystemIndices systemIndices;
    private final Client client;

    public SystemIndexManager(SystemIndices systemIndices, Client client) {
        this.systemIndices = systemIndices;
        this.client = client;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        final ClusterState state = event.state();
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait until the gateway has recovered from disk, otherwise we may think we don't have some
            // indices but they may not have been restored from the cluster state on disk
            logger.debug("system indices manager waiting until state has been recovered");
            return;
        }

        // If this node is not a master node, exit.
        if (state.nodes().isLocalNodeElectedMaster() == false) {
            return;
        }

        for (SystemIndexDescriptor descriptor : this.systemIndices.getSystemIndexDescriptors()) {
            if (descriptor.isAutomaticallyManaged()) {
                upgradeIndexMetadataIfNecessary(state, descriptor);
            }
        }
    }

    private void upgradeIndexMetadataIfNecessary(ClusterState state, SystemIndexDescriptor descriptor) {
        final Metadata metadata = state.metadata();
        final String indexName = descriptor.getPrimaryIndex();
        final String aliasName = descriptor.getAliasName();

        if (metadata.hasIndex(indexName) == false) {
            // System indices are created on-demand
            return;
        }

        final State indexState = calculateIndexState(state, descriptor);
        final String concreteIndexName = indexState.concreteIndexName;

        if (indexState.isIndexUpToDate == false) {
            logger.debug(
                "Index [{}] (alias [{}]) is not on the current version. "
                    + "Features relying on the index will not be available until the index is upgraded",
                concreteIndexName,
                aliasName
            );
        } else if (indexState.mappingUpToDate == false) {
            logger.info("Index [{}] (alias [{}]) mappings are not up to date and will be updated", concreteIndexName, aliasName);
            PutMappingRequest request = new PutMappingRequest(concreteIndexName).source(descriptor.getMappings(), XContentType.JSON);

            final OriginSettingClient originSettingClient = new OriginSettingClient(this.client, descriptor.getOrigin());

            originSettingClient.admin().indices().putMapping(request, new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    if (acknowledgedResponse.isAcknowledged() == false) {
                        logger.error("Put mapping request for [{}] was not acknowledged", concreteIndexName);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("Put mapping request for [" + concreteIndexName + "] failed", e);
                }
            });
        } else {
            logger.trace("Index [{}] (alias [{}]) is up-to-date, no action required", concreteIndexName, aliasName);
        }
    }

    private State calculateIndexState(ClusterState state, SystemIndexDescriptor descriptor) {
        final IndexMetadata indexMetadata = resolveConcreteIndex(state.metadata(), descriptor.getPrimaryIndex());
        assert indexMetadata != null;

        final boolean isIndexUpToDate = INDEX_FORMAT_SETTING.get(indexMetadata.getSettings()) == descriptor.getIndexFormat();

        final boolean isMappingIsUpToDate = checkIndexMappingUpToDate(descriptor, indexMetadata);
        final String concreteIndexName = indexMetadata.getIndex().getName();

        if (indexMetadata.getState() == IndexMetadata.State.CLOSE) {
            logger.warn(
                "Index [{}] (alias [{}]) is closed. This is likely to prevent some features from functioning correctly",
                concreteIndexName,
                descriptor.getAliasName()
            );
        }

        return new State(isIndexUpToDate, isMappingIsUpToDate, concreteIndexName);
    }

    /**
     * Resolves a concrete index name or alias to a {@link IndexMetadata} instance.  Requires
     * that if supplied with an alias, the alias resolves to at most one concrete index.
     */
    private IndexMetadata resolveConcreteIndex(final Metadata metadata, final String aliasOrIndexName) {
        final IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(aliasOrIndexName);
        if (indexAbstraction != null) {
            final List<IndexMetadata> indices = indexAbstraction.getIndices();
            if (indexAbstraction.getType() != IndexAbstraction.Type.CONCRETE_INDEX && indices.size() > 1) {
                throw new IllegalStateException(
                    "Alias ["
                        + aliasOrIndexName
                        + "] points to more than one index: "
                        + indices.stream().map(imd -> imd.getIndex().getName()).collect(Collectors.toList())
                );
            }
            return indices.get(0);
        }
        return null;
    }

    private boolean checkIndexMappingUpToDate(SystemIndexDescriptor descriptor, IndexMetadata indexMetadata) {
        final MappingMetadata mappingMetadata = indexMetadata.mapping();
        Set<Version> versions = mappingMetadata == null ? Set.of() : Set.of(readMappingVersion(descriptor, mappingMetadata));
        return versions.stream().allMatch(Version.CURRENT::equals);
    }

    @SuppressWarnings("unchecked")
    private Version readMappingVersion(SystemIndexDescriptor descriptor, MappingMetadata mappingMetadata) {
        final String indexName = descriptor.getIndexPattern();
        try {
            Map<String, Object> meta = (Map<String, Object>) mappingMetadata.sourceAsMap().get("_meta");
            if (meta == null) {
                logger.info("Missing _meta field in mapping [{}] of index [{}]", mappingMetadata.type(), indexName);
                throw new IllegalStateException("Cannot read version string in index " + indexName);
            }
            return Version.fromString((String) meta.get(descriptor.getVersionMetaKey()));
        } catch (ElasticsearchParseException e) {
            logger.error(new ParameterizedMessage("Cannot parse the mapping for index [{}]", indexName), e);
            throw new ElasticsearchException("Cannot parse the mapping for index [{}]", e, indexName);
        }
    }

    private static class State {
        final boolean isIndexUpToDate;
        final boolean mappingUpToDate;
        final String concreteIndexName;

        State(boolean isIndexUpToDate, boolean mappingUpToDate, String concreteIndexName) {
            this.isIndexUpToDate = isIndexUpToDate;
            this.mappingUpToDate = mappingUpToDate;
            this.concreteIndexName = concreteIndexName;
        }
    }
}
