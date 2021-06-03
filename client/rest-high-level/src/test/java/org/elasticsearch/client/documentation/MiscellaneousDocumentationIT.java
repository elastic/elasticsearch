/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.documentation;

import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.MainResponse;
import org.elasticsearch.client.xpack.XPackInfoRequest;
import org.elasticsearch.client.xpack.XPackInfoResponse;
import org.elasticsearch.client.xpack.XPackInfoResponse.BuildInfo;
import org.elasticsearch.client.xpack.XPackInfoResponse.FeatureSetsInfo;
import org.elasticsearch.client.xpack.XPackInfoResponse.LicenseInfo;
import org.elasticsearch.client.xpack.XPackUsageRequest;
import org.elasticsearch.client.xpack.XPackUsageResponse;

import java.io.IOException;
import java.time.Instant;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

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
            String clusterName = response.getClusterName();
            String clusterUuid = response.getClusterUuid();
            String nodeName = response.getNodeName();
            MainResponse.Version version = response.getVersion();
            String buildDate = version.getBuildDate();
            String buildFlavor = version.getBuildFlavor();
            String buildHash = version.getBuildHash();
            String buildType = version.getBuildType();
            String luceneVersion = version.getLuceneVersion();
            String minimumIndexCompatibilityVersion= version.getMinimumIndexCompatibilityVersion();
            String minimumWireCompatibilityVersion = version.getMinimumWireCompatibilityVersion();
            String number = version.getNumber();
            //end::main-response
            assertNotNull(clusterName);
            assertNotNull(clusterUuid);
            assertNotNull(nodeName);
            assertNotNull(version);
            assertNotNull(buildDate);
            assertNotNull(buildFlavor);
            assertNotNull(buildHash);
            assertNotNull(buildType);
            assertNotNull(luceneVersion);
            assertNotNull(minimumIndexCompatibilityVersion);
            assertNotNull(minimumWireCompatibilityVersion);
            assertNotNull(number);
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
            XPackInfoResponse response = client.xpack().info(request, RequestOptions.DEFAULT);
            //end::x-pack-info-execute

            //tag::x-pack-info-response
            BuildInfo build = response.getBuildInfo();                 // <1>
            LicenseInfo license = response.getLicenseInfo();           // <2>
            assertThat(license.getExpiryDate(), is(greaterThan(Instant.now().toEpochMilli())));  // <3>
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
            client.xpack().infoAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::x-pack-info-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testXPackUsage() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            //tag::x-pack-usage-execute
            XPackUsageRequest request = new XPackUsageRequest();
            XPackUsageResponse response = client.xpack().usage(request, RequestOptions.DEFAULT);
            //end::x-pack-usage-execute

            //tag::x-pack-usage-response
            Map<String, Map<String, Object>> usages = response.getUsages();
            Map<String, Object> monitoringUsage = usages.get("monitoring");
            assertThat(monitoringUsage.get("available"), is(true));
            assertThat(monitoringUsage.get("enabled"), is(true));
            assertThat(monitoringUsage.get("collection_enabled"), is(false));
            //end::x-pack-usage-response
        }
        {
            XPackUsageRequest request = new XPackUsageRequest();
            // tag::x-pack-usage-execute-listener
            ActionListener<XPackUsageResponse> listener = new ActionListener<XPackUsageResponse>() {
                @Override
                public void onResponse(XPackUsageResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::x-pack-usage-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::x-pack-usage-execute-async
            client.xpack().usageAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::x-pack-usage-execute-async

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
