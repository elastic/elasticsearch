/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.RequestValidators;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.RemoteClusterStateRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.RemoteClusterClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.ccr.CcrSettings;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public final class CcrRequests {

    private CcrRequests() {}

    public static RemoteClusterStateRequest metadataRequest(String leaderIndex) {
        RemoteClusterStateRequest clusterStateRequest = new RemoteClusterStateRequest(TimeValue.MAX_VALUE);
        clusterStateRequest.clear();
        clusterStateRequest.metadata(true);
        clusterStateRequest.indices(leaderIndex);
        return clusterStateRequest;
    }

    /**
     * Resolves the current name of {@code index} on the remote cluster by its UUID. A renamed index keeps its UUID, so this is how a
     * follower finds a leader whose name changed after it was captured. It lists the shards of every index the caller may see,
     * without any stats, which is cheap but not free, so it is only used after a request by name came back with nothing.
     */
    public static void resolveIndexByUuid(RemoteClusterClient client, Index index, ActionListener<Index> listener) {
        // no stats sections: only the shard routing, which carries the index name and uuid, is needed
        final IndicesStatsRequest request = new IndicesStatsRequest().indices("*").clear();
        request.indicesOptions(IndicesOptions.lenientExpandHidden());
        client.execute(IndicesStatsAction.REMOTE_TYPE, request, listener.delegateFailureAndWrap((delegate, response) -> {
            for (ShardStats shardStats : response.getShards()) {
                final Index candidate = shardStats.getShardRouting().index();
                if (candidate.getUUID().equals(index.getUUID())) {
                    delegate.onResponse(candidate);
                    return;
                }
            }
            delegate.onFailure(new IndexNotFoundException(index));
        }));
    }

    public static PutMappingRequest putMappingRequest(String followerIndex, MappingMetadata mappingMetadata) {
        PutMappingRequest putMappingRequest = new PutMappingRequest(followerIndex);
        putMappingRequest.origin("ccr");
        putMappingRequest.source(mappingMetadata.source().string());
        putMappingRequest.masterNodeTimeout(TimeValue.MAX_VALUE);
        return putMappingRequest;
    }

    /**
     * Gets an {@link IndexMetadata} of the given index. The mapping version and metadata version of the returned {@link IndexMetadata}
     * must be at least the provided {@code mappingVersion} and {@code metadataVersion} respectively.
     */
    public static void getIndexMetadata(
        RemoteClusterClient client,
        Index index,
        long mappingVersion,
        long metadataVersion,
        Supplier<TimeValue> timeoutSupplier,
        ActionListener<IndexMetadata> listener
    ) {
        final RemoteClusterStateRequest request = CcrRequests.metadataRequest(index.getName());
        if (metadataVersion > 0) {
            request.waitForMetadataVersion(metadataVersion).waitForTimeout(timeoutSupplier.get());
        }
        client.execute(ClusterStateAction.REMOTE_TYPE, request, listener.delegateFailureAndWrap((delegate, response) -> {
            if (response.getState() == null) { // timeout on wait_for_metadata_version
                assert metadataVersion > 0 : metadataVersion;
                if (timeoutSupplier.get().nanos() < 0) {
                    delegate.onFailure(
                        new IllegalStateException(
                            "timeout to get cluster state with"
                                + " metadata version ["
                                + metadataVersion
                                + "], mapping version ["
                                + mappingVersion
                                + "]"
                        )
                    );
                } else {
                    getIndexMetadata(client, index, mappingVersion, metadataVersion, timeoutSupplier, delegate);
                }
            } else {
                final Metadata metadata = response.getState().metadata();
                final IndexMetadata indexMetadata = metadata.getProject().getIndexSafe(index);
                if (indexMetadata.getMappingVersion() >= mappingVersion) {
                    delegate.onResponse(indexMetadata);
                    return;
                }
                if (timeoutSupplier.get().nanos() < 0) {
                    delegate.onFailure(
                        new IllegalStateException("timeout to get cluster state with mapping version [" + mappingVersion + "]")
                    );
                } else {
                    // ask for the next version.
                    getIndexMetadata(client, index, mappingVersion, metadata.version() + 1, timeoutSupplier, delegate);
                }
            }
        }));
    }

    public static final RequestValidators.RequestValidator<PutMappingRequest> CCR_PUT_MAPPING_REQUEST_VALIDATOR = (
        request,
        projectMetadata,
        indices) -> {
        if (request.origin() == null) {
            return Optional.empty(); // a put-mapping-request on old versions does not have origin.
        }
        final List<Index> followingIndices = Arrays.stream(indices).filter(index -> {
            final IndexMetadata indexMetadata = projectMetadata.index(index);
            return indexMetadata != null && CcrSettings.CCR_FOLLOWING_INDEX_SETTING.get(indexMetadata.getSettings());
        }).toList();
        if (followingIndices.isEmpty() == false && "ccr".equals(request.origin()) == false) {
            final String errorMessage = "can't put mapping to the following indices "
                + "["
                + followingIndices.stream().map(Index::getName).collect(Collectors.joining(", "))
                + "]; "
                + "the mapping of the following indices are self-replicated from its leader indices";
            return Optional.of(new ElasticsearchStatusException(errorMessage, RestStatus.FORBIDDEN));
        }
        return Optional.empty();
    };

    public static final RequestValidators.RequestValidator<IndicesAliasesRequest> CCR_INDICES_ALIASES_REQUEST_VALIDATOR = (
        request,
        projectMetadata,
        indices) -> {
        if (request.origin() == null) {
            return Optional.empty(); // an indices aliases request on old versions does not have origin
        }
        final List<Index> followingIndices = Arrays.stream(indices).filter(index -> {
            final IndexMetadata indexMetadata = projectMetadata.index(index);
            return indexMetadata != null && CcrSettings.CCR_FOLLOWING_INDEX_SETTING.get(indexMetadata.getSettings());
        }).toList();
        if (followingIndices.isEmpty() == false && "ccr".equals(request.origin()) == false) {
            final String errorMessage = "can't modify aliases on indices "
                + "["
                + followingIndices.stream().map(Index::getName).collect(Collectors.joining(", "))
                + "]; "
                + "aliases of following indices are self-replicated from their leader indices";
            return Optional.of(new ElasticsearchStatusException(errorMessage, RestStatus.FORBIDDEN));
        }
        return Optional.empty();
    };

}
