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

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.snapshots.clone.CloneSnapshotRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * Clones indices from one snapshot into another
 */
public class RestCloneSnapshotAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(PUT, "/_snapshot/{repository}/{snapshot}/_clone/{target_snapshot}"));
    }

    @Override
    public String getName() {
        return "clone_snapshot_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        Map<String, Object> body = request.contentParser().mapOrdered();
        final Object indexSettings = body.get("index_settings");
        final CloneSnapshotRequest cloneSnapshotRequest = new CloneSnapshotRequest(
                request.param("repository"), request.param("snapshot"), request.param("target_snapshot"),
                XContentMapValues.nodeStringArrayValue(body.getOrDefault("indices", Collections.emptyList())),
                XContentMapValues.nodeStringArrayValue(body.getOrDefault("excluded_settings", Collections.emptyList())),
                indexSettings == null ? Settings.EMPTY :
                        Settings.builder().loadFromMap(XContentMapValues.nodeMapValue(indexSettings, "index_settings")).build());
        cloneSnapshotRequest.masterNodeTimeout(request.paramAsTime("master_timeout", cloneSnapshotRequest.masterNodeTimeout()));
        return channel -> client.admin().cluster().cloneSnapshot(cloneSnapshotRequest, new RestToXContentListener<>(channel));
    }
}
