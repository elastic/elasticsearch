/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.smoketest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

public class SmokeTestPluginsSslClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    private static final String USER = "test_user";
    private static final String PASS = "x-pack-test-password";

    public SmokeTestPluginsSslClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters();
    }

    static Path certificateAuthorities;

    @BeforeClass
    public static void getKeyStore() {
        try {
            certificateAuthorities = PathUtils.get(SmokeTestPluginsSslClientYamlTestSuiteIT.class.getResource("/testnode.crt").toURI());
        } catch (URISyntaxException e) {
            throw new ElasticsearchException("exception while reading the store", e);
        }
        if (Files.exists(certificateAuthorities) == false) {
            throw new IllegalStateException("Keystore file [" + certificateAuthorities + "] does not exist.");
        }
    }

    @AfterClass
    public static void clearKeyStore() {
        certificateAuthorities = null;
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .put(ESRestTestCase.CERTIFICATE_AUTHORITIES, certificateAuthorities)
            .build();
    }

    @Override
    protected String getProtocol() {
        return "https";
    }
}
