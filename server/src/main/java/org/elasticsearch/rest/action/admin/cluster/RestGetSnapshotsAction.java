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

import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.elasticsearch.client.Requests.getSnapshotsRequest;
import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Returns information about snapshot
 */
public class RestGetSnapshotsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_snapshot/{repository}/{snapshot}"));
    }

    @Override
    public String getName() {
        return "get_snapshots_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String[] repositories = request.paramAsStringArray("repository", Strings.EMPTY_ARRAY);
        String[] snapshots = request.paramAsStringArray("snapshot", Strings.EMPTY_ARRAY);

        GetSnapshotsRequest getSnapshotsRequest = getSnapshotsRequest(repositories).snapshots(snapshots);
        getSnapshotsRequest.ignoreUnavailable(request.paramAsBoolean("ignore_unavailable", getSnapshotsRequest.ignoreUnavailable()));
        getSnapshotsRequest.verbose(request.paramAsBoolean("verbose", getSnapshotsRequest.verbose()));
        getSnapshotsRequest.masterNodeTimeout(request.paramAsTime("master_timeout", getSnapshotsRequest.masterNodeTimeout()));
        return channel -> client.admin().cluster().getSnapshots(getSnapshotsRequest, new RestToXContentListener<>(channel));
    }
}
