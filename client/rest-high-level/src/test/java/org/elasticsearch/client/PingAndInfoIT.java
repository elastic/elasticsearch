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

import org.elasticsearch.action.main.MainResponse;

import java.io.IOException;
import java.util.Map;

public class PingAndInfoIT extends ESRestHighLevelClientTestCase {

    public void testPing() throws IOException {
        assertTrue(highLevelClient().ping());
    }

    @SuppressWarnings("unchecked")
    public void testInfo() throws IOException {
        MainResponse info = highLevelClient().info();
        // compare with what the low level client outputs
        Map<String, Object> infoAsMap = entityAsMap(adminClient().performRequest("GET", "/"));
        assertEquals(infoAsMap.get("cluster_name"), info.getClusterName().value());
        assertEquals(infoAsMap.get("cluster_uuid"), info.getClusterUuid());

        // only check node name existence, might be a different one from what was hit by low level client in multi-node cluster
        assertNotNull(info.getNodeName());
        Map<String, Object> versionMap = (Map<String, Object>) infoAsMap.get("version");
        assertEquals(versionMap.get("build_hash"), info.getBuild().shortHash());
        assertEquals(versionMap.get("build_date"), info.getBuild().date());
        assertEquals(versionMap.get("build_snapshot"), info.getBuild().isSnapshot());
        assertEquals(versionMap.get("number"), info.getVersion().toString());
        assertEquals(versionMap.get("lucene_version"), info.getVersion().luceneVersion.toString());
    }

}
