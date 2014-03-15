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

package org.elasticsearch.rest.action.admin.cluster.snapshots.status;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequest;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;

import static org.elasticsearch.client.Requests.snapshotsStatusRequest;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;

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
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        String repository = request.param("repository");
        String[] snapshots = request.paramAsStringArray("snapshot", Strings.EMPTY_ARRAY);
        if (snapshots.length == 1 && "_all".equalsIgnoreCase(snapshots[0])) {
            snapshots = Strings.EMPTY_ARRAY;
        }
        SnapshotsStatusRequest snapshotsStatusResponse = snapshotsStatusRequest(repository).snapshots(snapshots);

        snapshotsStatusResponse.masterNodeTimeout(request.paramAsTime("master_timeout", snapshotsStatusResponse.masterNodeTimeout()));
        client.admin().cluster().snapshotsStatus(snapshotsStatusResponse, new AbstractRestResponseActionListener<SnapshotsStatusResponse>(request, channel, logger) {
            @Override
            public void onResponse(SnapshotsStatusResponse response) {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject();
                    response.toXContent(builder, request);
                    builder.endObject();
                    channel.sendResponse(new XContentRestResponse(request, OK, builder));
                } catch (IOException e) {
                    onFailure(e);
                }
            }
        });
    }
}
