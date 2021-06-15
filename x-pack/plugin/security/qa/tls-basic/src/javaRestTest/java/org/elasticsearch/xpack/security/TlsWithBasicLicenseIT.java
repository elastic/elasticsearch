/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ObjectPath;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;

public class TlsWithBasicLicenseIT extends ESRestTestCase {
    private static Path httpTrustStore;

    @BeforeClass
    public static void findTrustStore() throws Exception {
        final URL resource = TlsWithBasicLicenseIT.class.getResource("/ssl/ca.p12");
        if (resource == null) {
            throw new FileNotFoundException("Cannot find classpath resource /ssl/ca.p12");
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
        String token = basicAuthHeaderValue("admin", new SecureString("admin-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .put(TRUSTSTORE_PATH, httpTrustStore)
            .put(TRUSTSTORE_PASSWORD, "password")
            .build();
    }

    public void testWithBasicLicense() throws Exception {
        checkLicenseType("basic");
        checkSSLEnabled();
        checkCertificateAPI();
    }

    public void testWithTrialLicense() throws Exception {
        startTrial();
        try {
            checkLicenseType("trial");
            checkSSLEnabled();
            checkCertificateAPI();
        } finally {
            revertTrial();
        }
    }

    private void startTrial() throws IOException {
        Response response = client().performRequest(new Request("POST", "/_license/start_trial?acknowledge=true"));
        assertOK(response);
    }

    private void revertTrial() throws IOException {
        client().performRequest(new Request("POST", "/_license/start_basic?acknowledge=true"));
    }

    private void checkLicenseType(String type) throws Exception {
        assertBusy(() -> {
            try {
                Map<String, Object> license = getAsMap("/_license");
                assertThat(license, notNullValue());
                assertThat(ObjectPath.evaluate(license, "license.type"), equalTo(type));
            } catch (ResponseException e) {
                throw new AssertionError(e);
            }
        });

    }

    private void checkSSLEnabled() throws IOException {
        Map<String, Object> usage = getAsMap("/_xpack/usage");
        assertThat(usage, notNullValue());
        assertThat(ObjectPath.evaluate(usage, "security.ssl.http.enabled"), equalTo(true));
        assertThat(ObjectPath.evaluate(usage, "security.ssl.transport.enabled"), equalTo(true));
    }

    @SuppressWarnings("unchecked")
    private void checkCertificateAPI() throws IOException {
        Response response = client().performRequest(new Request("GET", "/_ssl/certificates"));
        ObjectPath path = ObjectPath.createFromResponse(response);
        final Object body = path.evaluate("");
        assertThat(body, instanceOf(List.class));
        final List<?> certs = (List<?>) body;
        assertThat(certs, iterableWithSize(3));
        final List<Map<String, Object>> certInfo = new ArrayList<>();
        for (int i = 0; i < certs.size(); i++) {
            final Object element = certs.get(i);
            assertThat(element, instanceOf(Map.class));
            final Map<String, Object> map = (Map<String, Object>) element;
            certInfo.add(map);
            assertThat(map.get("format"), equalTo("PEM"));
        }
        List<String> paths = certInfo.stream().map(m -> String.valueOf(m.get("path"))).collect(Collectors.toList());
        assertThat(paths, containsInAnyOrder("http.crt", "transport.crt", "ca.crt"));
    }


}

