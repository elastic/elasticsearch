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

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.admin.indices.upgrade.get.UpgradeStatusRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.action.admin.indices.RestUpgradeActionDeprecated.UPGRADE_API_DEPRECATION_MESSAGE;

public class RestUpgradeStatusActionDeprecated extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(
        LogManager.getLogger(RestUpgradeStatusActionDeprecated.class));

    @Override
    public List<DeprecatedRoute> deprecatedRoutes() {
        return unmodifiableList(asList(
            new DeprecatedRoute(GET, "/_upgrade", UPGRADE_API_DEPRECATION_MESSAGE, deprecationLogger),
            new DeprecatedRoute(GET, "/{index}/_upgrade", UPGRADE_API_DEPRECATION_MESSAGE, deprecationLogger)));
    }

    @Override
    public List<Route> routes() {
        return Collections.emptyList();
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        UpgradeStatusRequest statusRequest = new UpgradeStatusRequest(Strings.splitStringByCommaToArray(request.param("index")));
        statusRequest.indicesOptions(IndicesOptions.fromRequest(request, statusRequest.indicesOptions()));
        return channel -> client.admin().indices().upgradeStatus(statusRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public String getName() {
        return "upgrade_status_action";
    }
}
