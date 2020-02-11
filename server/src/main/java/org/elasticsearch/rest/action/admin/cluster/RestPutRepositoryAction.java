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

import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.client.Requests.putRepositoryRequest;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * Registers repositories
 */
public class RestPutRepositoryAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(
            new Route(POST, "/_snapshot/{repository}"),
            new Route(PUT, "/_snapshot/{repository}")));
    }

    @Override
    public String getName() {
        return "put_repository_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        PutRepositoryRequest putRepositoryRequest = putRepositoryRequest(request.param("repository"));
        try (XContentParser parser = request.contentParser()) {
            putRepositoryRequest.source(parser.mapOrdered());
        }
        putRepositoryRequest.verify(request.paramAsBoolean("verify", true));
        putRepositoryRequest.masterNodeTimeout(request.paramAsTime("master_timeout", putRepositoryRequest.masterNodeTimeout()));
        putRepositoryRequest.timeout(request.paramAsTime("timeout", putRepositoryRequest.timeout()));
        return channel -> client.admin().cluster().putRepository(putRepositoryRequest, new RestToXContentListener<>(channel));
    }
}
