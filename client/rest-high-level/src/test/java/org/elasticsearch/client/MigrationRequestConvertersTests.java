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

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.client.migration.IndexUpgradeInfoRequest;
import org.elasticsearch.client.migration.IndexUpgradeRequest;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

public class MigrationRequestConvertersTests extends ESTestCase {

    public void testGetMigrationAssistance() {
        IndexUpgradeInfoRequest upgradeInfoRequest = new IndexUpgradeInfoRequest();
        String expectedEndpoint = "/_migration/assistance";
        if (randomBoolean()) {
            String[] indices = RequestConvertersTests.randomIndicesNames(1, 5);
            upgradeInfoRequest.indices(indices);
            expectedEndpoint += "/" + String.join(",", indices);
        }
        Map<String, String> expectedParams = new HashMap<>();
        RequestConvertersTests.setRandomIndicesOptions(upgradeInfoRequest::indicesOptions, upgradeInfoRequest::indicesOptions,
            expectedParams);
        Request request = MigrationRequestConverters.getMigrationAssistance(upgradeInfoRequest);
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals(expectedEndpoint, request.getEndpoint());
        assertNull(request.getEntity());
        assertEquals(expectedParams, request.getParameters());
    }

    public void testUpgradeRequest() {
        String[] indices = RequestConvertersTests.randomIndicesNames(1, 1);
        IndexUpgradeRequest upgradeInfoRequest = new IndexUpgradeRequest(indices[0]);

        String expectedEndpoint = "/_migration/upgrade/" + indices[0];
        Map<String, String> expectedParams = new HashMap<>();
        expectedParams.put("wait_for_completion", Boolean.TRUE.toString());

        Request request = MigrationRequestConverters.migrate(upgradeInfoRequest);

        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals(expectedEndpoint, request.getEndpoint());
        assertNull(request.getEntity());
        assertEquals(expectedParams, request.getParameters());
    }
}
