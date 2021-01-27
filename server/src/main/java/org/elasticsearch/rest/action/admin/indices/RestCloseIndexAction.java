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

import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestCloseIndexAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestCloseIndexAction.class);

    public static final String WAIT_FOR_ACTIVE_SHARDS_DEFAULT_DEPRECATION_MESSAGE = "the default value for the ?wait_for_active_shards " +
            "parameter will change from '0' to 'index-setting' in version 8; specify '?wait_for_active_shards=index-setting' " +
            "to adopt the future default behaviour, or '?wait_for_active_shards=0' to preserve today's behaviour";

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(
            new Route(POST, "/_close"),
            new Route(POST, "/{index}/_close")));
    }

    @Override
    public String getName() {
        return "close_index_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        CloseIndexRequest closeIndexRequest = new CloseIndexRequest(Strings.splitStringByCommaToArray(request.param("index")));
        closeIndexRequest.masterNodeTimeout(request.paramAsTime("master_timeout", closeIndexRequest.masterNodeTimeout()));
        closeIndexRequest.timeout(request.paramAsTime("timeout", closeIndexRequest.timeout()));
        closeIndexRequest.indicesOptions(IndicesOptions.fromRequest(request, closeIndexRequest.indicesOptions()));
        String waitForActiveShards = request.param("wait_for_active_shards");
        if (waitForActiveShards == null) {
            deprecationLogger.deprecate(DeprecationCategory.API, "close-index-wait_for_active_shards-default",
                WAIT_FOR_ACTIVE_SHARDS_DEFAULT_DEPRECATION_MESSAGE);
        } else if ("index-setting".equalsIgnoreCase(waitForActiveShards)) {
            closeIndexRequest.waitForActiveShards(ActiveShardCount.DEFAULT);
        } else {
            closeIndexRequest.waitForActiveShards(ActiveShardCount.parseString(waitForActiveShards));
        }
        return channel -> client.admin().indices().close(closeIndexRequest, new RestToXContentListener<>(channel));
    }

}
