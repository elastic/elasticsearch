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

import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestUpgradeAction extends BaseRestHandler {

    public RestUpgradeAction(RestController controller) {
        controller.registerHandler(POST, "/_upgrade", this);
        controller.registerHandler(POST, "/{index}/_upgrade", this);
    }

    @Override
    public String getName() {
        return "upgrade_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        UpgradeRequest upgradeReq = new UpgradeRequest(Strings.splitStringByCommaToArray(request.param("index")));
        upgradeReq.indicesOptions(IndicesOptions.fromRequest(request, upgradeReq.indicesOptions()));
        upgradeReq.upgradeOnlyAncientSegments(request.paramAsBoolean("only_ancient_segments", false));
        return channel -> client.admin().indices().upgrade(upgradeReq, new RestToXContentListener<>(channel));
    }
}
