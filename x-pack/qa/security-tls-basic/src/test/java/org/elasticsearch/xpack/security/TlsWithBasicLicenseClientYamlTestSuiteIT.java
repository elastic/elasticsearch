/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.FileNotFoundException;
import java.net.URL;
import java.nio.file.Path;

public class TlsWithBasicLicenseClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {
    private static Path httpTrustStore;

    public TlsWithBasicLicenseClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters();
    }

    @BeforeClass
    public static void findTrustStore( ) throws Exception {
        final URL resource = TlsWithBasicLicenseClientYamlTestSuiteIT.class.getResource("/ssl/ca.p12");
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
        return Settings.builder()
                .put(TRUSTSTORE_PATH , httpTrustStore)
                .put(TRUSTSTORE_PASSWORD, "password")
                .build();
    }
}

