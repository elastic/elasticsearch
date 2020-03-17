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

import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.client.Requests.snapshotsStatusRequest;
import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Returns status of currently running snapshot
 */
public class RestSnapshotsStatusAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(
            new Route(GET, "/_snapshot/{repository}/{snapshot}/_status"),
            new Route(GET, "/_snapshot/{repository}/_status"),
            new Route(GET, "/_snapshot/_status")));
    }

    @Override
    public String getName() {
        return "snapshot_status_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String repository = request.param("repository", "_all");
        String[] snapshots = request.paramAsStringArray("snapshot", Strings.EMPTY_ARRAY);
        if (snapshots.length == 1 && "_all".equalsIgnoreCase(snapshots[0])) {
            snapshots = Strings.EMPTY_ARRAY;
        }
        SnapshotsStatusRequest snapshotsStatusRequest = snapshotsStatusRequest(repository).snapshots(snapshots);
        snapshotsStatusRequest.ignoreUnavailable(request.paramAsBoolean("ignore_unavailable", snapshotsStatusRequest.ignoreUnavailable()));

        snapshotsStatusRequest.masterNodeTimeout(request.paramAsTime("master_timeout", snapshotsStatusRequest.masterNodeTimeout()));
        return channel -> client.admin().cluster().snapshotsStatus(snapshotsStatusRequest, new RestToXContentListener<>(channel));
    }
}
