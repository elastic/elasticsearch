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
package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.AcknowledgedRestListener;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

/**
 */
public class RestIndexDeleteAliasesAction extends BaseRestHandler {

    @Inject
    public RestIndexDeleteAliasesAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(DELETE, "/{index}/_alias/{name}", this);
        controller.registerHandler(DELETE, "/{index}/_aliases/{name}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final NodeClient client) {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final String[] aliases = Strings.splitStringByCommaToArray(request.param("name"));
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        indicesAliasesRequest.timeout(request.paramAsTime("timeout", indicesAliasesRequest.timeout()));
        indicesAliasesRequest.removeAlias(indices, aliases);
        indicesAliasesRequest.masterNodeTimeout(request.paramAsTime("master_timeout", indicesAliasesRequest.masterNodeTimeout()));

        client.admin().indices().aliases(indicesAliasesRequest, new AcknowledgedRestListener<IndicesAliasesResponse>(channel));
    }
}
