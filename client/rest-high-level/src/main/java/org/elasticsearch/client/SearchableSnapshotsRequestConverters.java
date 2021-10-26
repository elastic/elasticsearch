/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.client.searchable_snapshots.CachesStatsRequest;
import org.elasticsearch.client.searchable_snapshots.MountSnapshotRequest;

import java.io.IOException;

import static org.elasticsearch.client.RequestConverters.REQUEST_BODY_CONTENT_TYPE;
import static org.elasticsearch.client.RequestConverters.createEntity;

final class SearchableSnapshotsRequestConverters {

    static Request mountSnapshot(final MountSnapshotRequest mountSnapshotRequest) throws IOException {
        final String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPartAsIs("_snapshot")
            .addPathPart(mountSnapshotRequest.getRepository())
            .addPathPart(mountSnapshotRequest.getSnapshot())
            .addPathPartAsIs("_mount")
            .build();
        final Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        final RequestConverters.Params parameters = new RequestConverters.Params();
        if (mountSnapshotRequest.getMasterTimeout() != null) {
            parameters.withMasterTimeout(mountSnapshotRequest.getMasterTimeout());
        }
        if (mountSnapshotRequest.getWaitForCompletion() != null) {
            parameters.withWaitForCompletion(mountSnapshotRequest.getWaitForCompletion());
        }
        if (mountSnapshotRequest.getStorage() != null) {
            parameters.putParam("storage", mountSnapshotRequest.getStorage().storageName());
        }
        request.addParameters(parameters.asMap());
        request.setEntity(createEntity(mountSnapshotRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request cacheStats(final CachesStatsRequest cacheStatsRequest) {
        final RequestConverters.EndpointBuilder endpoint = new RequestConverters.EndpointBuilder()
            .addPathPartAsIs("_searchable_snapshots");
        if (cacheStatsRequest.getNodesIds() != null) {
            endpoint.addCommaSeparatedPathParts(cacheStatsRequest.getNodesIds());
        }
        endpoint.addPathPartAsIs("cache", "stats");
        return new Request(HttpGet.METHOD_NAME, endpoint.build());
    }
}
