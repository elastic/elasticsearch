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
import org.elasticsearch.client.core.MainResponse;
import org.elasticsearch.client.xpack.XPackInfoRequest;
import org.elasticsearch.client.xpack.XPackInfoResponse;
import org.elasticsearch.client.xpack.XPackInfoResponse.FeatureSetsInfo.FeatureSet;
import org.elasticsearch.client.license.LicenseStatus;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;

public class PingAndInfoIT extends ESRestHighLevelClientTestCase {

    public void testPing() throws IOException {
        assertTrue(highLevelClient().ping(RequestOptions.DEFAULT));
    }

    public void testInfo() throws IOException {
        MainResponse info = highLevelClient().info(RequestOptions.DEFAULT);
        // compare with what the low level client outputs
        Map<String, Object> infoAsMap = entityAsMap(adminClient().performRequest(new Request(HttpGet.METHOD_NAME, "/")));
        assertEquals(infoAsMap.get("cluster_name"), info.ClusterName());
        assertEquals(infoAsMap.get("cluster_uuid"), info.ClusterUuid());

        // only check node name existence, might be a different one from what was hit by low level client in multi-node cluster
        assertNotNull(info.NodeName());
        @SuppressWarnings("unchecked")
        Map<String, Object> versionMap = (Map<String, Object>) infoAsMap.get("version");
        assertEquals(versionMap.get("build_flavor"), info.Version().getBuildFlavor());
        assertEquals(versionMap.get("build_type"), info.Version().getBuildType());
        assertEquals(versionMap.get("build_hash"), info.Version().getBuildHash());
        assertEquals(versionMap.get("build_date"), info.Version().getBuildDate());
        assertEquals(versionMap.get("build_snapshot"), info.Version().isSnapshot());
        assertTrue(versionMap.get("number").toString().startsWith(info.Version().getNumber()));
        assertEquals(versionMap.get("lucene_version"), info.Version().getLuceneVersion());
    }

    public void testXPackInfo() throws Exception {
        waitForActiveLicense(client());
        XPackInfoRequest request = new XPackInfoRequest();
        request.setCategories(EnumSet.allOf(XPackInfoRequest.Category.class));
        request.setVerbose(true);
        XPackInfoResponse info = highLevelClient().xpack().info(request, RequestOptions.DEFAULT);

        MainResponse mainResponse = highLevelClient().info(RequestOptions.DEFAULT);

        assertEquals(mainResponse.Version().getBuildHash(), info.getBuildInfo().getHash());

        assertEquals("trial", info.getLicenseInfo().getType());
        assertEquals("trial", info.getLicenseInfo().getMode());
        assertEquals(LicenseStatus.ACTIVE, info.getLicenseInfo().getStatus());

        FeatureSet graph = info.getFeatureSetsInfo().getFeatureSets().get("graph");
        assertTrue(graph.available());
        assertTrue(graph.enabled());
        FeatureSet monitoring = info.getFeatureSetsInfo().getFeatureSets().get("monitoring");
        assertTrue(monitoring.available());
        assertTrue(monitoring.enabled());
        FeatureSet ml = info.getFeatureSetsInfo().getFeatureSets().get("ml");
        assertTrue(ml.available());
        assertTrue(ml.enabled());
    }

    public void testXPackInfoEmptyRequest() throws IOException {
        XPackInfoResponse info = highLevelClient().xpack().info(new XPackInfoRequest(), RequestOptions.DEFAULT);

        // TODO: reconsider this leniency now that the transport client is gone
        assertNull(info.getBuildInfo());
        assertNull(info.getLicenseInfo());
        assertNull(info.getFeatureSetsInfo());
    }
}
