/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.xcontent.XContentType;

public final class CcrRequests {

    private CcrRequests() {}

    public static ClusterStateRequest metaDataRequest(String leaderIndex) {
        ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.clear();
        clusterStateRequest.metaData(true);
        clusterStateRequest.indices(leaderIndex);
        return clusterStateRequest;
    }

    public static PutMappingRequest putMappingRequest(String followerIndex, MappingMetaData mappingMetaData) {
        PutMappingRequest putMappingRequest = new PutMappingRequest(followerIndex);
        putMappingRequest.type(mappingMetaData.type());
        putMappingRequest.source(mappingMetaData.source().string(), XContentType.JSON);
        return putMappingRequest;
    }
}
