/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ssl;

import org.apache.commons.io.FilenameUtils;
import org.apache.lucene.util.Constants;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslConfigException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.junit.Before;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.AccessControlException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.elasticsearch.test.SecuritySettingsSource.addSecureSettings;
import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.instanceOf;

/**
 * This is a suite of tests to ensure that meaningful error messages are generated for defined SSL configuration problems due to file
 * problems.
 * These messages are not fixed, and there are not BWC concerns with changing them. It is entirely acceptable to improve the messages in
 * minor releases (but improvements only - not backwards steps).
 */
public class SSLErrorMessageFileTests extends ESTestCase {

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
        checkMissingKeyManagerResource("[jks] keystore", "keystore.path", null);
    }

    public void testMessageForMissingPemCertificate() {
        checkMissingKeyManagerResource("PEM certificate", "certificate", withKey("cert1a.key"));
    }

    public void testMessageForMissingPemKey() {
        checkMissingKeyManagerResource("PEM private key", "key", withCertificate("cert1a.crt"));
    }

    public void testMessageForMissingTruststore() {
        checkMissingTrustManagerResource("[jks] keystore (as a truststore)", "truststore.path");
    }

    public void testMessageForMissingCertificateAuthorities() {
        checkMissingTrustManagerResource("PEM certificate_authorities", "certificate_authorities");
    }

    public void testMessageForKeystoreWithoutReadAccess() throws Exception {
        checkUnreadableKeyManagerResource("cert1a.p12", "[PKCS12] keystore", "keystore.path", null);
    }

    public void testMessageForPemCertificateWithoutReadAccess() throws Exception {
        checkUnreadableKeyManagerResource("cert1a.crt", "PEM certificate", "certificate", withKey("cert1a.key"));
    }

    public void testMessageForPemKeyWithoutReadAccess() throws Exception {
        checkUnreadableKeyManagerResource("cert1a.key", "PEM private key", "key", withCertificate("cert1a.crt"));
    }

    public void testMessageForTruststoreWithoutReadAccess() throws Exception {
        checkUnreadableTrustManagerResource("cert1a.p12", "[PKCS12] keystore (as a truststore)", "truststore.path");
    }

    public void testMessageForCertificateAuthoritiesWithoutReadAccess() throws Exception {
        checkUnreadableTrustManagerResource("ca1.crt", "PEM certificate_authorities", "certificate_authorities");
    }

    public void testMessageForKeyStoreOutsideConfigDir() throws Exception {
        checkBlockedKeyManagerResource("[jks] keystore", "keystore.path", null);
    }

    public void testMessageForPemCertificateOutsideConfigDir() throws Exception {
        checkBlockedKeyManagerResource("PEM certificate", "certificate", withKey("cert1a.key"));
    }

    public void testMessageForPemKeyOutsideConfigDir() throws Exception {
        checkBlockedKeyManagerResource("PEM private key", "key", withCertificate("cert1a.crt"));
    }

    public void testMessageForTrustStoreOutsideConfigDir() throws Exception {
        checkBlockedTrustManagerResource("[jks] keystore (as a truststore)", "truststore.path");
    }

    public void testMessageForCertificateAuthoritiesOutsideConfigDir() throws Exception {
        checkBlockedTrustManagerResource("PEM certificate_authorities", "certificate_authorities");
    }

    public void testMessageForTransportSslEnabledWithoutKeys() throws Exception {
        final String prefix = "xpack.security.transport.ssl";
        final Settings.Builder settings = Settings.builder();
        settings.put(prefix + ".enabled", true);
        if (inFipsJvm()) {
            configureWorkingTrustedAuthorities(prefix, settings);
        } else {
            configureWorkingTruststore(prefix, settings);
        }

        Throwable exception = expectFailure(settings);
        assertThat(
            exception,
            throwableWithMessage(
                "invalid SSL configuration for "
                    + prefix
                    + " - server ssl configuration requires a key and certificate, but these have not been configured;"
                    + " you must set either ["
                    + prefix
                    + ".keystore.path], or both ["
                    + prefix
                    + ".key] and ["
                    + prefix
                    + ".certificate]"
            )
        );
        assertThat(exception, instanceOf(ElasticsearchException.class));
    }

    public void testNoErrorIfTransportSslDisabledWithoutKeys() throws Exception {
        final String prefix = "xpack.security.transport.ssl";
        final Settings.Builder settings = Settings.builder();
        settings.put(prefix + ".enabled", false);
        if (inFipsJvm()) {
            configureWorkingTrustedAuthorities(prefix, settings);
        } else {
            configureWorkingTruststore(prefix, settings);
        }
        expectSuccess(settings);
    }

    public void testMessageForTransportNotEnabledButKeystoreConfigured() throws Exception {
        assumeFalse("Cannot run in a FIPS JVM since it uses a PKCS12 keystore", inFipsJvm());
        final String prefix = "xpack.security.transport.ssl";
        checkUnusedConfiguration(prefix, prefix + ".keystore.path," + prefix + ".keystore.secure_password", this::configureWorkingKeystore);
    }

    public void testMessageForTransportNotEnabledButTruststoreConfigured() throws Exception {
        assumeFalse("Cannot run in a FIPS JVM since it uses a PKCS12 keystore", inFipsJvm());
        final String prefix = "xpack.security.transport.ssl";
        checkUnusedConfiguration(
            prefix,
            prefix + ".truststore.path," + prefix + ".truststore.secure_password",
            this::configureWorkingTruststore
        );
    }

    public void testMessageForHttpsNotEnabledButKeystoreConfigured() throws Exception {
        assumeFalse("Cannot run in a FIPS JVM since it uses a PKCS12 keystore", inFipsJvm());
        final String prefix = "xpack.security.http.ssl";
        checkUnusedConfiguration(prefix, prefix + ".keystore.path," + prefix + ".keystore.secure_password", this::configureWorkingKeystore);
    }

    public void testMessageForHttpsNotEnabledButTruststoreConfigured() throws Exception {
        assumeFalse("Cannot run in a FIPS JVM since it uses a PKCS12 keystore", inFipsJvm());
        final String prefix = "xpack.security.http.ssl";
        checkUnusedConfiguration(
            prefix,
            prefix + ".truststore.path," + prefix + ".truststore.secure_password",
            this::configureWorkingTruststore
        );
    }

    private void checkMissingKeyManagerResource(String fileType, String configKey, @Nullable Settings.Builder additionalSettings) {
        checkMissingResource(fileType, configKey, (prefix, builder) -> buildKeyConfigSettings(additionalSettings, prefix, builder));
    }

    private void buildKeyConfigSettings(@Nullable Settings.Builder additionalSettings, String prefix, Settings.Builder builder) {
        if (inFipsJvm()) {
            configureWorkingTrustedAuthorities(prefix, builder);
        } else {
            configureWorkingTruststore(prefix, builder);
        }
        if (additionalSettings != null) {
            builder.put(additionalSettings.normalizePrefix(prefix + ".").build());
        }
    }

    private void checkMissingTrustManagerResource(String fileType, String configKey) {
        checkMissingResource(fileType, configKey, this::configureWorkingKeystore);
    }

    private void checkUnreadableKeyManagerResource(
        String fromResource,
        String fileType,
        String configKey,
        @Nullable Settings.Builder additionalSettings
    ) throws Exception {
        checkUnreadableResource(
            fromResource,
            fileType,
            configKey,
            (prefix, builder) -> buildKeyConfigSettings(additionalSettings, prefix, builder)
        );
    }

    private void checkUnreadableTrustManagerResource(String fromResource, String fileType, String configKey) throws Exception {
        checkUnreadableResource(fromResource, fileType, configKey, this::configureWorkingKeystore);
    }

    private void checkBlockedKeyManagerResource(String fileType, String configKey, Settings.Builder additionalSettings) throws Exception {
        checkBlockedResource(
            "KeyManager",
            fileType,
            configKey,
            (prefix, builder) -> buildKeyConfigSettings(additionalSettings, prefix, builder)
        );
    }

    private void checkBlockedTrustManagerResource(String fileType, String configKey) throws Exception {
        checkBlockedResource("TrustManager", fileType, configKey, this::configureWorkingKeystore);
    }

    private void checkMissingResource(String fileType, String configKey, BiConsumer<String, Settings.Builder> configure) {
        final String prefix = randomSslPrefix();
        final Settings.Builder settings = Settings.builder();
        configure.accept(prefix, settings);

        final String fileName = missingFile();
        final String key = prefix + "." + configKey;
        settings.put(key, fileName);

        final String fileErrorMessage = "cannot read configured " + fileType + " [" + fileName + "] because the file does not exist";
        Throwable exception = expectFailure(settings);
        assertThat(exception, throwableWithMessage("failed to load SSL configuration [" + prefix + "] - " + fileErrorMessage));
        assertThat(exception, instanceOf(ElasticsearchSecurityException.class));

        exception = exception.getCause();
        assertThat(exception, throwableWithMessage(fileErrorMessage));
        assertThat(exception, instanceOf(SslConfigException.class));

        exception = exception.getCause();
        assertThat(exception, instanceOf(NoSuchFileException.class));
        assertThat(exception, throwableWithMessage(fileName));
    }

    private void checkUnreadableResource(
        String fromResource,
        String fileType,
        String configKey,
        BiConsumer<String, Settings.Builder> configure
    ) throws Exception {
        final String prefix = randomSslPrefix();
        final Settings.Builder settings = Settings.builder();
        configure.accept(prefix, settings);

        final String fileName = unreadableFile(fromResource);
        final String key = prefix + "." + configKey;
        settings.put(key, fileName);

        final String fileErrorMessage = "not permitted to read the " + fileType + " file [" + fileName + "]";

        Throwable exception = expectFailure(settings);
        assertThat(exception, throwableWithMessage("failed to load SSL configuration [" + prefix + "] - " + fileErrorMessage));
        assertThat(exception, instanceOf(ElasticsearchSecurityException.class));

        exception = exception.getCause();
        assertThat(exception, throwableWithMessage(fileErrorMessage));
        assertThat(exception, instanceOf(SslConfigException.class));

        exception = exception.getCause();
        assertThat(exception, instanceOf(AccessDeniedException.class));
        assertThat(exception, throwableWithMessage(fileName));
    }

    private void checkBlockedResource(
        String sslManagerType,
        String fileType,
        String configKey,
        BiConsumer<String, Settings.Builder> configure
    ) throws Exception {
        final String prefix = randomSslPrefix();
        final Settings.Builder settings = Settings.builder();
        configure.accept(prefix, settings);

        final String fileName = blockedFile();
        final String key = prefix + "." + configKey;
        settings.put(key, fileName);

        final String fileErrorMessage = "cannot read configured "
            + fileType
            + " ["
            + fileName
            + "] because access to read the file is blocked; SSL resources should be placed in the ["
            + env.configFile().toAbsolutePath().toString()
            + "] directory";

        Throwable exception = expectFailure(settings);
        assertThat(exception, throwableWithMessage("failed to load SSL configuration [" + prefix + "] - " + fileErrorMessage));
        assertThat(exception, instanceOf(ElasticsearchSecurityException.class));

        exception = exception.getCause();
        assertThat(exception, throwableWithMessage(fileErrorMessage));
        assertThat(exception, instanceOf(SslConfigException.class));

        exception = exception.getCause();
        assertThat(exception, instanceOf(AccessControlException.class));
        assertThat(exception, throwableWithMessage(containsString(fileName)));
    }

    private void checkUnusedConfiguration(String prefix, String settingsConfigured, BiConsumer<String, Settings.Builder> configure) {
        final Settings.Builder settings = Settings.builder();
        configure.accept(prefix, settings);

        Throwable exception = expectFailure(settings);
        assertThat(
            exception,
            throwableWithMessage(
                "invalid configuration for "
                    + prefix
                    + " - ["
                    + prefix
                    + ".enabled] is not set,"
                    + " but the following settings have been configured in elasticsearch.yml : ["
                    + settingsConfigured
                    + "]"
            )
        );
        assertThat(exception, instanceOf(ElasticsearchException.class));
    }

    private String missingFile() {
        return resource("cert1a.p12").replace("cert1a.p12", "file.dne");
    }

    private String unreadableFile(String fromResource) throws IOException {
        assumeFalse("This behaviour uses POSIX file permissions", Constants.WINDOWS);
        final Path fromPath = this.paths.get(fromResource);
        if (fromPath == null) {
            throw new IllegalArgumentException("Test SSL resource " + fromResource + " has not been loaded");
        }
        final String extension = FilenameUtils.getExtension(fromResource);
        return copy(fromPath, createTempFile(fromResource, "-no-read." + extension), PosixFilePermissions.fromString("---------"));
    }

    private String blockedFile() throws IOException {
        return PathUtils.get("/this", "path", "is", "outside", "the", "config", "directory", "file.error").toAbsolutePath().toString();
    }

    /**
     * This more-or-less replicates the functionality of {@link Files#copy(Path, Path, CopyOption...)} but doing it this way allows us to
     * set the file permissions when creating the file (which helps with security manager issues)
     */
    private String copy(Path fromPath, Path toPath, Set<PosixFilePermission> permissions) throws IOException {
        Files.deleteIfExists(toPath);
        final FileAttribute<Set<PosixFilePermission>> fileAttributes = PosixFilePermissions.asFileAttribute(permissions);
        final EnumSet<StandardOpenOption> options = EnumSet.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        try (
            SeekableByteChannel channel = Files.newByteChannel(toPath, options, fileAttributes);
            OutputStream out = Channels.newOutputStream(channel)
        ) {
            Files.copy(fromPath, out);
        }
        return toPath.toString();
    }

    private Settings.Builder withKey(String fileName) {
        assertThat(fileName, endsWith(".key"));
        return Settings.builder().put("key", resource(fileName));
    }

    private Settings.Builder withCertificate(String fileName) {
        assertThat(fileName, endsWith(".crt"));
        return Settings.builder().put("certificate", resource(fileName));
    }

    private Settings.Builder configureWorkingTruststore(String prefix, Settings.Builder settings) {
        settings.put(prefix + ".truststore.path", resource("cert1a.p12"));
        addSecureSettings(settings, secure -> secure.setString(prefix + ".truststore.secure_password", "cert1a-p12-password"));
        return settings;
    }

    private Settings.Builder configureWorkingTrustedAuthorities(String prefix, Settings.Builder settings) {
        settings.putList(prefix + ".certificate_authorities", resource("ca1.crt"));
        return settings;
    }

    private Settings.Builder configureWorkingKeystore(String prefix, Settings.Builder settings) {
        settings.put(prefix + ".keystore.path", resource("cert1a.p12"));
        addSecureSettings(settings, secure -> secure.setString(prefix + ".keystore.secure_password", "cert1a-p12-password"));
        return settings;
    }

    private ElasticsearchException expectFailure(Settings.Builder settings) {
        return expectThrows(
            ElasticsearchException.class,
            () -> new SSLService(new Environment(buildEnvSettings(settings.build()), env.configFile()))
        );
    }

    private SSLService expectSuccess(Settings.Builder settings) {
        return new SSLService(TestEnvironment.newEnvironment(buildEnvSettings(settings.build())));
    }

    private String resource(String fileName) {
        final Path path = this.paths.get(fileName);
        if (path == null) {
            throw new IllegalArgumentException("Test SSL resource " + fileName + " has not been loaded");
        }
        return path.toString();
    }

    private void requirePath(String fileName) throws FileNotFoundException {
        final Path path = getDataPath("/org/elasticsearch/xpack/ssl/SSLErrorMessageTests/" + fileName);
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
