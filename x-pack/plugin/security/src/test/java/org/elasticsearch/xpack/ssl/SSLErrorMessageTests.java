/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ssl;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.junit.Before;

import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.test.SecuritySettingsSource.addSecureSettings;
import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.hamcrest.Matchers.instanceOf;

/**
 * This is a suite of tests to ensure that meaningful error messages are generated for defined SSL configuration problems.
 */
public class SSLErrorMessageTests extends ESTestCase {

    private Environment env;
    private Map<String, Path> paths;

    @Before
    public void setup() throws Exception {
        env = TestEnvironment.newEnvironment(Settings.builder().put("path.home", createTempDir()).build());
        paths = new HashMap<>();

        requirePath("ca1.p12");
        requirePath("ca1.jks");
        requirePath("ca1.crt");

        requirePath("cert1a.p12");
        requirePath("cert1a.jks");
        requirePath("cert1a.crt");
        requirePath("cert1a.key");
    }

    public void testMessageForMissingKeystore() {
        final String prefix = randomSslPrefix();
        final Settings.Builder settings = configureWorkingTruststore(prefix, Settings.builder());

        final String fileName = missingFile();
        final String key = prefix + ".keystore.path";
        settings.put(key, fileName);

        Throwable exception = expectFailure(settings);
        assertThat(exception, throwableWithMessage("failed to load SSL configuration [" + prefix + "]"));
        assertThat(exception, instanceOf(ElasticsearchSecurityException.class));

        exception = exception.getCause();
        assertThat(exception, throwableWithMessage(
            "failed to initialize SSL KeyManagerFactory - keystore file [" + fileName + "] does not exist"));
        assertThat(exception, instanceOf(ElasticsearchException.class));

        exception = exception.getCause();
        assertThat(exception, instanceOf(NoSuchFileException.class));
        assertThat(exception, throwableWithMessage(fileName));
    }

    public void testMessageForMissingTruststore() {
        final String prefix = randomSslPrefix();
        final Settings.Builder settings = configureWorkingKeystore(prefix, Settings.builder());

        final String fileName = missingFile();
        final String key = prefix + ".truststore.path";
        settings.put(key, fileName);

        Throwable exception = expectFailure(settings);
        assertThat(exception, throwableWithMessage("failed to load SSL configuration [" + prefix + "]"));
        assertThat(exception, instanceOf(ElasticsearchSecurityException.class));

        exception = exception.getCause();
        assertThat(exception, throwableWithMessage(
            "failed to initialize SSL TrustManagerFactory - truststore file [" + fileName + "] does not exist"));
        assertThat(exception, instanceOf(ElasticsearchException.class));

        exception = exception.getCause();
        assertThat(exception, instanceOf(NoSuchFileException.class));
        assertThat(exception, throwableWithMessage(fileName));
    }

    public void testMessageForMissingPemCertificate() {
        final String prefix = randomSslPrefix();
        final Settings.Builder settings = configureWorkingTruststore(prefix, Settings.builder());
        settings.put(prefix + ".key", resource("cert1a.key"));

        final String fileName = missingFile();
        final String key = prefix + ".certificate";
        settings.put(key, fileName);

        Throwable exception = expectFailure(settings);
        assertThat(exception, throwableWithMessage("failed to load SSL configuration [" + prefix + "]"));
        assertThat(exception, instanceOf(ElasticsearchSecurityException.class));

        exception = exception.getCause();
        assertThat(exception, throwableWithMessage(
            "failed to initialize SSL KeyManagerFactory - certificate file [" + fileName + "] does not exist"));
        assertThat(exception, instanceOf(ElasticsearchException.class));

        exception = exception.getCause();
        assertThat(exception, instanceOf(NoSuchFileException.class));
        assertThat(exception, throwableWithMessage(fileName));
    }

    public void testMessageForMissingPemKey() {
        final String prefix = randomSslPrefix();
        final Settings.Builder settings = configureWorkingTruststore(prefix, Settings.builder());
        settings.put(prefix + ".certificate", resource("cert1a.crt"));

        final String fileName = missingFile();
        final String key = prefix + ".key";
        settings.put(key, fileName);

        Throwable exception = expectFailure(settings);
        assertThat(exception, throwableWithMessage("failed to load SSL configuration [" + prefix + "]"));
        assertThat(exception, instanceOf(ElasticsearchSecurityException.class));

        exception = exception.getCause();
        assertThat(exception, throwableWithMessage(
            "failed to initialize SSL KeyManagerFactory - key file [" + fileName + "] does not exist"));
        assertThat(exception, instanceOf(ElasticsearchException.class));

        exception = exception.getCause();
        assertThat(exception, instanceOf(NoSuchFileException.class));
        assertThat(exception, throwableWithMessage(fileName));
    }
    public void testMessageForMissingCertificateAuthorities() {
        final String prefix = randomSslPrefix();
        final Settings.Builder settings = configureWorkingKeystore(prefix, Settings.builder());

        final String fileName = missingFile();
        final String key = prefix + ".certificate_authorities";
        settings.putList(key, fileName);

        Throwable exception = expectFailure(settings);
        assertThat(exception, throwableWithMessage("failed to load SSL configuration [" + prefix + "]"));
        assertThat(exception, instanceOf(ElasticsearchSecurityException.class));

        exception = exception.getCause();
        assertThat(exception, throwableWithMessage(
            "failed to initialize SSL TrustManagerFactory - certificate_authorities file [" + fileName + "] does not exist"));
        assertThat(exception, instanceOf(ElasticsearchException.class));

        exception = exception.getCause();
        assertThat(exception, instanceOf(NoSuchFileException.class));
        assertThat(exception, throwableWithMessage(fileName));
    }

    private String missingFile() {
        return resource("cert1a.p12").replace("cert1a.p12", "file.dne");
    }

    private Settings.Builder configureWorkingTruststore(String prefix, Settings.Builder settings) {
        settings.put(prefix + ".truststore.path", resource("cert1a.p12"));
        addSecureSettings(settings, secure -> secure.setString(prefix + ".truststore.secure_password", "cert1a-p12-password"));
        return settings;
    }

    private Settings.Builder configureWorkingKeystore(String prefix, Settings.Builder settings) {
        settings.put(prefix + ".keystore.path", resource("cert1a.p12"));
        addSecureSettings(settings, secure -> secure.setString(prefix + ".keystore.secure_password", "cert1a-p12-password"));
        return settings;
    }

    private ElasticsearchException expectFailure(Settings.Builder settings) {
        return expectThrows(ElasticsearchException.class, () -> new SSLService(settings.build(), env));
    }

    private String resource(String fileName) {
        final Path path = this.paths.get(fileName);
        if (path == null) {
            throw new IllegalArgumentException("Test SSL resource " + fileName + " has not been loaded");
        }
        return path.toString();
    }

    private void requirePath(String fileName) throws FileNotFoundException {
        final Path path = getDataPath("/org/elasticsearch/xpack/ssl/SSLServiceErrorMessageTests/" + fileName);
        if (Files.exists(path)) {
            paths.put(fileName, path);
        } else {
            throw new FileNotFoundException("File " + path + " does not exist");
        }
    }

    private String randomSslPrefix() {
        return randomFrom(
            "xpack.security.transport.ssl",
            "xpack.security.http.ssl",
            "xpack.http.ssl",
            "xpack.security.authc.realms.ldap.ldap1.ssl",
            "xpack.security.authc.realms.saml.saml1.ssl",
            "xpack.monitoring.exporters.http.ssl"
        );
    }
}
