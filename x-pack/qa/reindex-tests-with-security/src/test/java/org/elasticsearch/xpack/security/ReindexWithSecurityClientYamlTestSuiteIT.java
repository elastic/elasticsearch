/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.FileNotFoundException;
import java.net.URL;
import java.nio.file.Path;

public class ReindexWithSecurityClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {
    private static final String USER = "test_admin";
    private static final String PASS = "x-pack-test-password";

    private static Path httpCertificateAuthority;

    public ReindexWithSecurityClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters();
    }

    @BeforeClass
    public static void findTrustedCaCertificate( ) throws Exception {
        final URL resource = ReindexWithSecurityClientYamlTestSuiteIT.class.getResource("/ssl/ca.crt");
        if (resource == null) {
            throw new FileNotFoundException("Cannot find classpath resource /ssl/ca.crt");
        }
        httpCertificateAuthority = PathUtils.get(resource.toURI());
    }

    @AfterClass
    public static void cleanupStatics() {
        httpCertificateAuthority = null;
    }

    @Override
    protected String getProtocol() {
        return "https";
    }

    /**
     * All tests run as a an administrative user but use <code>es-security-runas-user</code> to become a less privileged user.
     */
    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder()
                .put(ThreadContext.PREFIX + ".Authorization", token)
                .put(CERTIFICATE_AUTHORITIES , httpCertificateAuthority)
                .build();
    }
}

