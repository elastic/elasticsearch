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

package org.elasticsearch.rest.action.admin.cluster.snapshots.delete;

import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.AcknowledgedRestListener;

import static org.elasticsearch.client.Requests.deleteSnapshotRequest;
import static org.elasticsearch.rest.RestRequest.Method.DELETE;

/**
 * Deletes a snapshot
 */
public class RestDeleteSnapshotAction extends BaseRestHandler {

    @Inject
    public RestDeleteSnapshotAction(Settings settings, RestController controller, Client client) {
        super(settings, client);
        controller.registerHandler(DELETE, "/_snapshot/{repository}/{snapshot}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        DeleteSnapshotRequest deleteSnapshotRequest = deleteSnapshotRequest(request.param("repository"), request.param("snapshot"));
        deleteSnapshotRequest.masterNodeTimeout(request.paramAsTime("master_timeout", deleteSnapshotRequest.masterNodeTimeout()));
        client.admin().cluster().deleteSnapshot(deleteSnapshotRequest, new AcknowledgedRestListener<DeleteSnapshotResponse>(channel));
    }
}
