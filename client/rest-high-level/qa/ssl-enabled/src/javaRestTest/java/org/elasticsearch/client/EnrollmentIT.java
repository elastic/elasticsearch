/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.elasticsearch.client.security.KibanaEnrollmentResponse;
import org.elasticsearch.client.security.NodeEnrollmentResponse;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.PathUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.FileNotFoundException;
import java.net.URL;
import java.nio.file.Path;
import java.util.List;

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

public class EnrollmentIT  extends ESRestHighLevelClientTestCase {
    private static Path httpTrustStore;

    @BeforeClass
    public static void findTrustStore() throws Exception {
        final URL resource = EnrollmentIT.class.getResource("/httpCa.p12");
        if (resource == null) {
            throw new FileNotFoundException("Cannot find classpath resource /httpCa.p12");
        }
        httpTrustStore = PathUtils.get(resource.toURI());
    }

    @AfterClass
    public static void cleanupStatics() {
        httpTrustStore = null;
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
            .put(TRUSTSTORE_PATH, httpTrustStore)
            .put(TRUSTSTORE_PASSWORD, "password")
            .build();
    }

    @Override
    protected Settings restAdminSettings() {
        return restClientSettings();
    }

    public void testEnrollNode() throws Exception {
        final NodeEnrollmentResponse nodeEnrollmentResponse =
            execute(highLevelClient().security()::enrollNode, highLevelClient().security()::enrollNodeAsync, RequestOptions.DEFAULT);
        assertThat(nodeEnrollmentResponse, notNullValue());
        assertThat(nodeEnrollmentResponse.getHttpCaKey(), endsWith("K2S3vidA="));
        assertThat(nodeEnrollmentResponse.getHttpCaCert(), endsWith("LfkRjirc="));
        assertThat(nodeEnrollmentResponse.getTransportKey(), endsWith("1I+r8vOQ=="));
        assertThat(nodeEnrollmentResponse.getTransportCert(), endsWith("OpTdtgJo="));
        List<String> nodesAddresses = nodeEnrollmentResponse.getNodesAddresses();
        assertThat(nodesAddresses.size(), equalTo(2));
    }

    public void testEnrollKibana() throws Exception {
        KibanaEnrollmentResponse kibanaResponse =
            execute(highLevelClient().security()::enrollKibana, highLevelClient().security()::enrollKibanaAsync, RequestOptions.DEFAULT);
        assertThat(kibanaResponse, notNullValue());
        assertThat(kibanaResponse.getHttpCa()
            , endsWith("brcNC5xq6YE7C4/06nH7F6le4kE4Uo6c9fpkl4ehOxQxndNLn462tFF+8VBA8IftJ1PPWzqGxLsCTzM6p6w8sa+XhgNYglLfkRjirc="));
        assertNotNull(kibanaResponse.getTokenValue());
        assertNotNull(kibanaResponse.getTokenName(), startsWith("enroll-process-token-"));
    }
}
