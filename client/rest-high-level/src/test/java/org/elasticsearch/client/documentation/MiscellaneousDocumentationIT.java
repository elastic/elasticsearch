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

package org.elasticsearch.client.documentation;

import org.apache.http.HttpHost;
import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.protocol.xpack.XPackInfoRequest;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.BuildInfo;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.FeatureSetsInfo;
import org.elasticsearch.protocol.xpack.XPackInfoResponse.LicenseInfo;

import java.io.IOException;
import java.util.EnumSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Documentation for miscellaneous APIs in the high level java client.
 * Code wrapped in {@code tag} and {@code end} tags is included in the docs.
 */
public class MiscellaneousDocumentationIT extends ESRestHighLevelClientTestCase {

    public void testMain() throws IOException {
        RestHighLevelClient client = highLevelClient();
        {
            //tag::main-execute
            MainResponse response = client.info(RequestOptions.DEFAULT);
            //end::main-execute
            //tag::main-response
            ClusterName clusterName = response.getClusterName(); // <1>
            String clusterUuid = response.getClusterUuid(); // <2>
            String nodeName = response.getNodeName(); // <3>
            Version version = response.getVersion(); // <4>
            Build build = response.getBuild(); // <5>
            //end::main-response
            assertNotNull(clusterName);
            assertNotNull(clusterUuid);
            assertNotNull(nodeName);
            assertNotNull(version);
            assertNotNull(build);
        }
    }

    public void testPing() throws IOException {
        RestHighLevelClient client = highLevelClient();
        //tag::ping-execute
        boolean response = client.ping(RequestOptions.DEFAULT);
        //end::ping-execute
        assertTrue(response);
    }

    public void testXPackInfo() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            //tag::x-pack-info-execute
            XPackInfoRequest request = new XPackInfoRequest();
            request.setVerbose(true);          // <1>
            request.setCategories(EnumSet.of(  // <2>
                    XPackInfoRequest.Category.BUILD,
                    XPackInfoRequest.Category.LICENSE,
                    XPackInfoRequest.Category.FEATURES));
            XPackInfoResponse response = client.xPackInfo(request, RequestOptions.DEFAULT);
            //end::x-pack-info-execute

            //tag::x-pack-info-response
            BuildInfo build = response.getBuildInfo();                 // <1>
            LicenseInfo license = response.getLicenseInfo();           // <2>
            assertEquals(XPackInfoResponse.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS,
                    license.getExpiryDate());                          // <3>
            FeatureSetsInfo features = response.getFeatureSetsInfo();  // <4>
            //end::x-pack-info-response

            assertNotNull(response.getBuildInfo());
            assertNotNull(response.getLicenseInfo());
            assertNotNull(response.getFeatureSetsInfo());
        }
        {
            XPackInfoRequest request = new XPackInfoRequest();
            // tag::x-pack-info-execute-listener
            ActionListener<XPackInfoResponse> listener = new ActionListener<XPackInfoResponse>() {
                @Override
                public void onResponse(XPackInfoResponse indexResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::x-pack-info-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::x-pack-info-execute-async
            client.xPackInfoAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::x-pack-info-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testInitializationFromClientBuilder() throws IOException {
        //tag::rest-high-level-client-init
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http"),
                        new HttpHost("localhost", 9201, "http")));
        //end::rest-high-level-client-init

        //tag::rest-high-level-client-close
        client.close();
        //end::rest-high-level-client-close
    }
}
