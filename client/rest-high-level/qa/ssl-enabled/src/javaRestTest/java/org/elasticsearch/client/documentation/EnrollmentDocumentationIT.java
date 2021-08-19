/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.documentation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.security.KibanaEnrollmentResponse;
import org.elasticsearch.client.security.NodeEnrollmentResponse;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.PathUtils;
import org.junit.BeforeClass;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.startsWith;

public class EnrollmentDocumentationIT extends ESRestHighLevelClientTestCase {
    static Path HTTP_TRUSTSTORE;

    @BeforeClass
    public static void getResources() throws Exception {
        HTTP_TRUSTSTORE = PathUtils.get(EnrollmentDocumentationIT.class.getResource("/httpCa.p12").toURI());
    }

    @Override
    protected String getProtocol() {
        return "https";
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin_user", new SecureString("admin-password".toCharArray()));

        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .put(TRUSTSTORE_PATH, HTTP_TRUSTSTORE)
            .put(TRUSTSTORE_PASSWORD, "password")
            .build();
    }

    @Override
    protected Settings restAdminSettings() {
        return restClientSettings();
    }

    public void testNodeEnrollment() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            // tag::node-enrollment-execute
            NodeEnrollmentResponse response = client.security().enrollNode(RequestOptions.DEFAULT);
            // end::node-enrollment-execute

            // tag::node-enrollment-response
            String httpCaKey = response.getHttpCaKey(); // <1>
            String httpCaCert = response.getHttpCaCert(); // <2>
            String transportKey = response.getTransportKey(); // <3>
            String transportCert = response.getTransportCert(); // <4>
            List<String> nodesAddresses = response.getNodesAddresses();  // <5>
            // end::node-enrollment-response
        }

        {
            // tag::node-enrollment-execute-listener
            ActionListener<NodeEnrollmentResponse> listener =
                new ActionListener<NodeEnrollmentResponse>() {
                    @Override
                    public void onResponse(NodeEnrollmentResponse response) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }};
            // end::node-enrollment-execute-listener

            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::node-enrollment-execute-async
            client.security().enrollNodeAsync(RequestOptions.DEFAULT, listener);
            // end::node-enrollment-execute-async
            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testKibanaEnrollment() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            // tag::kibana-enrollment-execute
            KibanaEnrollmentResponse response = client.security().enrollKibana(RequestOptions.DEFAULT);
            // end::kibana-enrollment-execute

            // tag::kibana-enrollment-response
            String tokenName = response.getTokenName(); // <1>
            SecureString tokenValue = response.getTokenValue(); // <2>
            String httoCa = response.getHttpCa(); // <3>
            // end::kibana-enrollment-response
            assertNotNull(tokenValue);
            assertThat(tokenName, startsWith("enroll-process-token-"));
        }

        {
            // tag::kibana-enrollment-execute-listener
            ActionListener<KibanaEnrollmentResponse> listener =
                new ActionListener<KibanaEnrollmentResponse>() {
                    @Override
                    public void onResponse(KibanaEnrollmentResponse response) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }};
            // end::kibana-enrollment-execute-listener

            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::kibana-enrollment-execute-async
            client.security().enrollKibanaAsync(RequestOptions.DEFAULT, listener);
            // end::kibana-enrollment-execute-async
            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }
}
