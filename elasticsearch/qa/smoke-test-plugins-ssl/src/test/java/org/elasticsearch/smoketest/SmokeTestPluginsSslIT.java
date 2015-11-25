/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.smoketest;

import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.support.Headers;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.shield.ShieldPlugin;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.RestTestCandidate;
import org.elasticsearch.test.rest.client.RestClient;
import org.elasticsearch.test.rest.parser.RestTestParseException;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

public class SmokeTestPluginsSslIT extends ESRestTestCase {

    private static final String USER = "test_user";
    private static final String PASS = "changeme";
    private static final String KEYSTORE_PASS = "keypass";

    public SmokeTestPluginsSslIT(@Name("yaml") RestTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws IOException, RestTestParseException {
        return ESRestTestCase.createParameters(0, 1);
    }

    static Path keyStore;

    @BeforeClass
    public static void getKeyStore() {
      try {
          keyStore = PathUtils.get(SmokeTestPluginsSslIT.class.getResource("/test-node.jks").toURI());
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
        String token = basicAuthHeaderValue(USER, new SecuredString(PASS.toCharArray()));
        return Settings.builder()
                .put(Headers.PREFIX + ".Authorization", token)
                .put(RestClient.PROTOCOL, "https")
                .put(RestClient.TRUSTSTORE_PATH, keyStore)
                .put(RestClient.TRUSTSTORE_PASSWORD, KEYSTORE_PASS)
                .build();
    }

    @Override
    protected Settings externalClusterClientSettings() {
        return Settings.builder()
                .put("shield.user", USER + ":" + PASS)
                .put("shield.transport.ssl", true)
                .put("shield.ssl.keystore.path", keyStore)
                .put("shield.ssl.keystore.password", KEYSTORE_PASS)
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.<Class<? extends Plugin>>singleton(ShieldPlugin.class);
    }

}
