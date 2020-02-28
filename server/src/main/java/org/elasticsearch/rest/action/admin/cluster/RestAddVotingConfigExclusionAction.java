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

import org.elasticsearch.action.admin.cluster.configuration.AddVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.configuration.AddVotingConfigExclusionsRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.IOException;

public class RestAddVotingConfigExclusionAction extends BaseRestHandler {

    private static final TimeValue DEFAULT_TIMEOUT = TimeValue.timeValueSeconds(30L);
    private static final Logger logger = LogManager.getLogger(RestAddVotingConfigExclusionAction.class);
    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(logger);

    public RestAddVotingConfigExclusionAction(RestController controller) {
        // TODO This API is being deprecated.
        controller.registerHandler(RestRequest.Method.POST, "/_cluster/voting_config_exclusions/{node_name}", this);

        controller.registerHandler(RestRequest.Method.POST, "/_cluster/voting_config_exclusions", this);
    }

    @Override
    public String getName() {
        return "add_voting_config_exclusions_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        AddVotingConfigExclusionsRequest votingConfigExclusionsRequest = resolveVotingConfigExclusionsRequest(request);
        return channel -> client.execute(
            AddVotingConfigExclusionsAction.INSTANCE,
            votingConfigExclusionsRequest,
            new RestToXContentListener<>(channel)
        );
    }

    AddVotingConfigExclusionsRequest resolveVotingConfigExclusionsRequest(final RestRequest request) {
        String deprecatedNodeDescription = null;
        String nodeIds = null;
        String nodeNames = null;

        if (request.hasParam("node_name")) {
            DEPRECATION_LOGGER.deprecatedAndMaybeLog("add_voting_config_exclusion",
                "Using [node_name] for adding voting config exclustion will be removed in a future version. " +
                    "Please use [node_ids] or [node_names] instead");
            deprecatedNodeDescription = request.param("node_name");
        }

        if (request.hasParam("node_ids")){
            nodeIds = request.param("node_ids");
        }

        if (request.hasParam("node_names")){
            nodeNames = request.param("node_names");
        }

        if(!oneAndonlyOneIsSet(deprecatedNodeDescription, nodeIds, nodeNames)) {
            throw new IllegalArgumentException("Please set node identifiers correctly. " +
                "One and only one of [node_name], [node_names] and [node_ids] has to be set");
        }

        return new AddVotingConfigExclusionsRequest(
            Strings.splitStringByCommaToArray(deprecatedNodeDescription),
            Strings.splitStringByCommaToArray(nodeIds),
            Strings.splitStringByCommaToArray(nodeNames),
            TimeValue.parseTimeValue(request.param("timeout"), DEFAULT_TIMEOUT, getClass().getSimpleName() + ".timeout")
        );
    }

    private boolean oneAndonlyOneIsSet(String deprecatedNodeDescription, String nodeIds, String nodeNames) {
        if(Strings.hasText(deprecatedNodeDescription)) {
            return Strings.isNullOrEmpty(nodeIds) && Strings.isNullOrEmpty(nodeNames);
        }
        else if (Strings.hasText(nodeIds)) {
            return Strings.isNullOrEmpty(nodeNames);
        }
        else if (Strings.hasText(nodeNames)) {
            return true;
        }
        else {
            // none of the node identifiers are set
            return false;
        }

    }

}
