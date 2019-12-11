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

import org.elasticsearch.action.admin.cluster.configuration.AddVotingConfigExclusionsRequest;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.junit.Before;
import java.util.HashMap;
import java.util.Map;


public class RestAddVotingConfigExclusionActionTests extends RestActionTestCase {

    private RestAddVotingConfigExclusionAction action;

    @Before
    public void setupAction() {
        action = new RestAddVotingConfigExclusionAction(controller());
    }

    public void testResolveVotingConfigExclusionsRequest() {
        Map<String, String> params = new HashMap<>();
        params.put("node_name", "node-1,node-2,node-3");
        RestRequest deprecatedRequest = new FakeRestRequest.Builder(xContentRegistry())
            .withMethod(RestRequest.Method.PUT)
            .withPath("/_cluster/voting_config_exclusions")
            .withParams(params)
            .build();

        AddVotingConfigExclusionsRequest addVotingConfigExclusionsRequest = action.resolveVotingConfigExclusionsRequest(deprecatedRequest);
        String[] expected = {"node-1","node-2", "node-3"};
        assertArrayEquals(expected, addVotingConfigExclusionsRequest.getNodeDescriptions());
    }
}
