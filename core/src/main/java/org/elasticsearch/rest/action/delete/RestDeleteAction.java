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

package org.elasticsearch.rest.action.delete;

import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestStatusToXContentListener;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

/**
 *
 */
public class RestDeleteAction extends BaseRestHandler {

    @Inject
    public RestDeleteAction(Settings settings, RestController controller, Client client) {
        super(settings, client);
        controller.registerHandler(DELETE, "/{index}/{type}/{id}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        DeleteRequest deleteRequest = new DeleteRequest(request.param("index"), request.param("type"), request.param("id"));
        deleteRequest.routing(request.param("routing"));
        deleteRequest.parent(request.param("parent")); // order is important, set it after routing, so it will set the routing
        deleteRequest.timeout(request.paramAsTime("timeout", DeleteRequest.DEFAULT_TIMEOUT));
        deleteRequest.setRefreshPolicy(request.param("refresh"));
        deleteRequest.version(RestActions.parseVersion(request));
        deleteRequest.versionType(VersionType.fromString(request.param("version_type"), deleteRequest.versionType()));

        String consistencyLevel = request.param("consistency");
        if (consistencyLevel != null) {
            deleteRequest.consistencyLevel(WriteConsistencyLevel.fromString(consistencyLevel));
        }

        client.delete(deleteRequest, new RestStatusToXContentListener<>(channel));
    }
}
