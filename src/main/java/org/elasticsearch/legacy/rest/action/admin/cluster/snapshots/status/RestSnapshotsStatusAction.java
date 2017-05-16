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

package org.elasticsearch.legacy.rest.action.admin.cluster.snapshots.status;

import org.elasticsearch.legacy.action.admin.cluster.snapshots.status.SnapshotsStatusRequest;
import org.elasticsearch.legacy.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.legacy.client.Client;
import org.elasticsearch.legacy.common.Strings;
import org.elasticsearch.legacy.common.inject.Inject;
import org.elasticsearch.legacy.common.settings.Settings;
import org.elasticsearch.legacy.rest.BaseRestHandler;
import org.elasticsearch.legacy.rest.RestChannel;
import org.elasticsearch.legacy.rest.RestController;
import org.elasticsearch.legacy.rest.RestRequest;
import org.elasticsearch.legacy.rest.action.support.RestToXContentListener;

import static org.elasticsearch.legacy.client.Requests.snapshotsStatusRequest;
import static org.elasticsearch.legacy.rest.RestRequest.Method.GET;

/**
 * Returns status of currently running snapshot
 */
public class RestSnapshotsStatusAction extends BaseRestHandler {

    @Inject
    public RestSnapshotsStatusAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/_snapshot/{repository}/{snapshot}/_status", this);
        controller.registerHandler(GET, "/_snapshot/{repository}/_status", this);
        controller.registerHandler(GET, "/_snapshot/_status", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        String repository = request.param("repository", "_all");
        String[] snapshots = request.paramAsStringArray("snapshot", Strings.EMPTY_ARRAY);
        if (snapshots.length == 1 && "_all".equalsIgnoreCase(snapshots[0])) {
            snapshots = Strings.EMPTY_ARRAY;
        }
        SnapshotsStatusRequest snapshotsStatusResponse = snapshotsStatusRequest(repository).snapshots(snapshots);

        snapshotsStatusResponse.masterNodeTimeout(request.paramAsTime("master_timeout", snapshotsStatusResponse.masterNodeTimeout()));
        client.admin().cluster().snapshotsStatus(snapshotsStatusResponse, new RestToXContentListener<SnapshotsStatusResponse>(channel));
    }
}
