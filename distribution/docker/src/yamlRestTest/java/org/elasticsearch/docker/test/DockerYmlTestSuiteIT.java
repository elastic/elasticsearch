/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.docker.test;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

public class DockerYmlTestSuiteIT extends ESClientYamlSuiteTestCase {

    private static final String USER = "x_pack_rest_user";
    private static final String PASS = "x-pack-test-password";

    public DockerYmlTestSuiteIT(ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters();
    }

    @Override
    protected String getTestRestCluster() {
        String distribution = getDistribution();
        return new StringBuilder().append("localhost:")
            .append(getProperty("test.fixtures.elasticsearch-" + distribution + "-1.tcp.9200"))
            .append(",")
            .append("localhost:")
            .append(getProperty("test.fixtures.elasticsearch-" + distribution + "-2.tcp.9200"))
            .toString();
    }

    @Override
    protected boolean randomizeContentType() {
        return false;
    }

    private String getDistribution() {
        String distribution = System.getProperty("tests.distribution", "default");
        if (distribution.equals("oss") == false && distribution.equals("default") == false) {
            throw new IllegalArgumentException("supported values for tests.distribution are oss or default but it was " + distribution);
        }
        return distribution;
    }

    private boolean isOss() {
        return getDistribution().equals("oss");
    }

    private String getProperty(String key) {
        String value = System.getProperty(key);
        if (value == null) {
            throw new IllegalStateException(
                "Could not find system properties from test.fixtures. "
                    + "This test expects to run with the elasticsearch.test.fixtures Gradle plugin"
            );
        }
        return value;
    }

    @Before
    public void waitForCluster() throws IOException {
        super.initClient();
        Request health = new Request("GET", "/_cluster/health");
        health.addParameter("wait_for_nodes", "2");
        health.addParameter("wait_for_status", "yellow");
        client().performRequest(health);
    }

    static Path trustedCertFile;

    @BeforeClass
    public static void getTrustedCert() {
        try {
            trustedCertFile = PathUtils.get(DockerYmlTestSuiteIT.class.getResource("/testnode.crt").toURI());
        } catch (URISyntaxException e) {
            throw new ElasticsearchException("exception while reading the certificate", e);
        }

        if (Files.exists(trustedCertFile) == false) {
            throw new IllegalStateException("Certificate file [" + trustedCertFile + "] does not exist.");
        }
    }

    @AfterClass
    public static void clearTrustedCert() {
        trustedCertFile = null;
    }

    @Override
    protected Settings restClientSettings() {
        if (isOss()) {
            return super.restClientSettings();
        }
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .put(ESRestTestCase.CERTIFICATE_AUTHORITIES, trustedCertFile)
            .build();
    }

    @Override
    protected String getProtocol() {
        if (isOss()) {
            return "http";
        }
        return "https";
    }
}
