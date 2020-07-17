/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.RequestValidators;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.ccr.CcrSettings;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public final class CcrRequests {

    private CcrRequests() {}

    public static ClusterStateRequest metadataRequest(String leaderIndex) {
        ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.clear();
        clusterStateRequest.metadata(true);
        clusterStateRequest.indices(leaderIndex);
        return clusterStateRequest;
    }

    public static PutMappingRequest putMappingRequest(String followerIndex, MappingMetadata mappingMetadata) {
        PutMappingRequest putMappingRequest = new PutMappingRequest(followerIndex);
        putMappingRequest.origin("ccr");
        putMappingRequest.source(mappingMetadata.source().string(), XContentType.JSON);
        return putMappingRequest;
    }

    /**
     * Gets an {@link IndexMetadata} of the given index. The mapping version and metadata version of the returned {@link IndexMetadata}
     * must be at least the provided {@code mappingVersion} and {@code metadataVersion} respectively.
     */
    public static void getIndexMetadata(Client client, Index index, long mappingVersion, long metadataVersion,
                                        Supplier<TimeValue> timeoutSupplier, ActionListener<IndexMetadata> listener) {
        final ClusterStateRequest request = CcrRequests.metadataRequest(index.getName());
        if (metadataVersion > 0) {
            request.waitForMetadataVersion(metadataVersion).waitForTimeout(timeoutSupplier.get());
        }
        client.admin().cluster().state(request, ActionListener.wrap(
            response -> {
                if (response.getState() == null) { // timeout on wait_for_metadata_version
                    assert metadataVersion > 0 : metadataVersion;
                    if (timeoutSupplier.get().nanos() < 0) {
                        listener.onFailure(new IllegalStateException("timeout to get cluster state with" +
                            " metadata version [" + metadataVersion + "], mapping version [" + mappingVersion + "]"));
                    } else {
                        getIndexMetadata(client, index, mappingVersion, metadataVersion, timeoutSupplier, listener);
                    }
                } else {
                    final Metadata metadata = response.getState().metadata();
                    final IndexMetadata indexMetadata = metadata.getIndexSafe(index);
                    if (indexMetadata.getMappingVersion() >= mappingVersion) {
                        listener.onResponse(indexMetadata);
                        return;
                    }
                    if (timeoutSupplier.get().nanos() < 0) {
                        listener.onFailure(new IllegalStateException(
                            "timeout to get cluster state with mapping version [" + mappingVersion + "]"));
                    } else {
                        // ask for the next version.
                        getIndexMetadata(client, index, mappingVersion, metadata.version() + 1, timeoutSupplier, listener);
                    }
                }
            },
            listener::onFailure
        ));
    }

    public static final RequestValidators.RequestValidator<PutMappingRequest> CCR_PUT_MAPPING_REQUEST_VALIDATOR =
            (request, state, indices) -> {
                if (request.origin() == null) {
                    return Optional.empty(); // a put-mapping-request on old versions does not have origin.
                }
                final List<Index> followingIndices = Arrays.stream(indices)
                        .filter(index -> {
                            final IndexMetadata indexMetadata = state.metadata().index(index);
                            return indexMetadata != null && CcrSettings.CCR_FOLLOWING_INDEX_SETTING.get(indexMetadata.getSettings());
                        }).collect(Collectors.toList());
                if (followingIndices.isEmpty() == false && "ccr".equals(request.origin()) == false) {
                    final String errorMessage = "can't put mapping to the following indices "
                            + "[" + followingIndices.stream().map(Index::getName).collect(Collectors.joining(", ")) + "]; "
                            + "the mapping of the following indices are self-replicated from its leader indices";
                    return Optional.of(new ElasticsearchStatusException(errorMessage, RestStatus.FORBIDDEN));
                }
                return Optional.empty();
            };

    public static final RequestValidators.RequestValidator<IndicesAliasesRequest> CCR_INDICES_ALIASES_REQUEST_VALIDATOR =
            (request, state, indices) -> {
                if (request.origin() == null) {
                    return Optional.empty(); // an indices aliases request on old versions does not have origin
                }
                final List<Index> followingIndices = Arrays.stream(indices)
                        .filter(index -> {
                            final IndexMetadata indexMetadata = state.metadata().index(index);
                            return indexMetadata != null && CcrSettings.CCR_FOLLOWING_INDEX_SETTING.get(indexMetadata.getSettings());
                        }).collect(Collectors.toList());
                if (followingIndices.isEmpty() == false && "ccr".equals(request.origin()) == false) {
                    final String errorMessage = "can't modify aliases on indices "
                            + "[" + followingIndices.stream().map(Index::getName).collect(Collectors.joining(", ")) + "]; "
                            + "aliases of following indices are self-replicated from their leader indices";
                    return Optional.of(new ElasticsearchStatusException(errorMessage, RestStatus.FORBIDDEN));
                }
                return Optional.empty();
            };

}
