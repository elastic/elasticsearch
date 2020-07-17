/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.smoketest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;

public class SmokeTestPluginsSslClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    private static final String USER = "test_user";
    private static final String PASS = "x-pack-test-password";
    private static final String KEYSTORE_PASS = "testnode";

    public SmokeTestPluginsSslClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters();
    }

    static Path keyStore;

    @BeforeClass
    public static void muteInFips() {
        assumeFalse("https://github.com/elastic/elasticsearch/issues/49094", inFipsJvm());
    }

    @BeforeClass
    public static void getKeyStore() {
      try {
          keyStore = PathUtils.get(SmokeTestPluginsSslClientYamlTestSuiteIT.class.getResource("/testnode.jks").toURI());
      } catch (URISyntaxException e) {
          throw new ElasticsearchException("exception while reading the store", e);
      }
      if (!Files.exists(keyStore)) {
          throw new IllegalStateException("Keystore file [" + keyStore + "] does not exist.");
      }
    }

    @AfterClass
    public static void clearKeyStore() {
      keyStore = null;
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder()
                .put(ThreadContext.PREFIX + ".Authorization", token)
                .put(ESRestTestCase.TRUSTSTORE_PATH, keyStore)
                .put(ESRestTestCase.TRUSTSTORE_PASSWORD, KEYSTORE_PASS)
                .build();
    }

    @Override
    protected String getProtocol() {
        return "https";
    }
}
