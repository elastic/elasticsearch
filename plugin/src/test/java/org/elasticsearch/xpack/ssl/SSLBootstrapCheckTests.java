/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ssl;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;

public class SSLBootstrapCheckTests extends ESTestCase {

    public void testSSLBootstrapCheckWithNoKey() throws Exception {
        SSLService sslService = new SSLService(Settings.EMPTY, null);
        SSLBootstrapCheck bootstrapCheck = new SSLBootstrapCheck(sslService, Settings.EMPTY, null);
        assertTrue(bootstrapCheck.check());
    }

    public void testSSLBootstrapCheckWithKey() throws Exception {
        final String keyPrefix = randomBoolean() ? "security.transport." : "";
        Settings settings = Settings.builder()
                .put("path.home", createTempDir())
                .put("xpack." + keyPrefix + "ssl.key",
                        getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.pem"))
                .put("xpack." + keyPrefix + "ssl.key_passphrase", "testclient")
                .put("xpack." + keyPrefix + "ssl.certificate",
                        getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt"))
                .build();
        final Environment env = randomBoolean() ? new Environment(settings) : null;
        SSLBootstrapCheck bootstrapCheck = new SSLBootstrapCheck(new SSLService(settings, env), settings, env);
        assertFalse(bootstrapCheck.check());
    }

    public void testSSLBootstrapCheckWithDefaultCABeingTrusted() throws Exception {
        final String keyPrefix = randomBoolean() ? "security.transport." : "";
        Settings settings = Settings.builder()
                .put("path.home", createTempDir())
                .put("xpack." + keyPrefix + "ssl.key",
                        getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.pem"))
                .put("xpack." + keyPrefix + "ssl.key_passphrase", "testclient")
                .put("xpack." + keyPrefix + "ssl.certificate",
                        getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt"))
                .putArray("xpack." + keyPrefix + "ssl.certificate_authorities",
                        getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt").toString(),
                        getDataPath("/org/elasticsearch/xpack/ssl/ca.pem").toString())
                .build();
        final Environment env = randomBoolean() ? new Environment(settings) : null;
        SSLBootstrapCheck bootstrapCheck = new SSLBootstrapCheck(new SSLService(settings, env), settings, env);
        assertTrue(bootstrapCheck.check());

        settings = Settings.builder().put(settings.filter((s) -> s.contains(".certificate_authorities")))
                .put("xpack.security.http.ssl.certificate_authorities",
                        getDataPath("/org/elasticsearch/xpack/ssl/ca.pem").toString())
                .build();
        bootstrapCheck = new SSLBootstrapCheck(new SSLService(settings, env), settings, env);
        assertTrue(bootstrapCheck.check());
    }

    public void testSSLBootstrapCheckWithDefaultKeyBeingUsed() throws Exception {
        final String keyPrefix = randomBoolean() ? "security.transport." : "";
        Settings settings = Settings.builder()
                .put("path.home", createTempDir())
                .put("xpack." + keyPrefix + "ssl.key",
                        getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.pem"))
                .put("xpack." + keyPrefix + "ssl.key_passphrase", "testclient")
                .put("xpack." + keyPrefix + "ssl.certificate",
                        getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt"))
                .put("xpack.security.http.ssl.key", getDataPath("/org/elasticsearch/xpack/ssl/private.pem").toString())
                .put("xpack.security.http.ssl.certificate", getDataPath("/org/elasticsearch/xpack/ssl/ca.pem").toString())
                .build();
        final Environment env = randomBoolean() ? new Environment(settings) : null;
        SSLBootstrapCheck bootstrapCheck = new SSLBootstrapCheck(new SSLService(settings, env), settings, env);
        assertTrue(bootstrapCheck.check());

        settings = Settings.builder().put(settings.filter((s) -> s.contains(".http.ssl.")))
                .put("xpack.security.transport.profiles.foo.xpack.security.ssl.key",
                        getDataPath("/org/elasticsearch/xpack/ssl/private.pem").toString())
                .put("xpack.security.transport.profiles.foo.xpack.security.ssl.certificate",
                        getDataPath("/org/elasticsearch/xpack/ssl/ca.pem").toString())
                .build();
        bootstrapCheck = new SSLBootstrapCheck(new SSLService(settings, env), settings, env);
        assertTrue(bootstrapCheck.check());
    }
}
