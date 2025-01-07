/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.actions.email;

import org.apache.http.ssl.SSLContextBuilder;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.watcher.notification.email.EmailService;
import org.elasticsearch.xpack.watcher.notification.email.EmailTemplate;
import org.elasticsearch.xpack.watcher.notification.email.HtmlSanitizer;
import org.elasticsearch.xpack.watcher.notification.email.support.EmailServer;
import org.elasticsearch.xpack.watcher.test.MockTextTemplateEngine;
import org.elasticsearch.xpack.watcher.test.WatcherTestUtils;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;

import static org.hamcrest.Matchers.hasSize;

public class EmailSslTests extends ESTestCase {

    private EmailServer server;
    private TextTemplateEngine textTemplateEngine = new MockTextTemplateEngine();
    private HtmlSanitizer htmlSanitizer = new HtmlSanitizer(Settings.EMPTY);

    @Before
    public void startSmtpServer() throws GeneralSecurityException, IOException {
        // Keystore and private key will share the same password
        final char[] keystorePassword = "test-smtp".toCharArray();
        final Path tempDir = createTempDir();
        final Path certPath = tempDir.resolve("test-smtp.crt");
        final Path keyPath = tempDir.resolve("test-smtp.pem");
        Files.copy(getDataPath("/org/elasticsearch/xpack/watcher/actions/email/test-smtp.crt"), certPath);
        Files.copy(getDataPath("/org/elasticsearch/xpack/watcher/actions/email/test-smtp.pem"), keyPath);
        KeyStore keyStore = CertParsingUtils.getKeyStoreFromPEM(certPath, keyPath, keystorePassword);
        final SSLContext sslContext = new SSLContextBuilder().loadKeyMaterial(keyStore, keystorePassword).build();
        server = EmailServer.localhost(logger, sslContext);
    }

    @After
    public void stopSmtpServer() {
        if (null != server) {
            server.stop();
        }
    }

    public void testFailureSendingMessageToSmtpServerWithUntrustedCertificateAuthority() throws Exception {
        final Settings.Builder settings = Settings.builder();
        final MockSecureSettings secureSettings = new MockSecureSettings();
        final ExecutableEmailAction emailAction = buildEmailAction(settings, secureSettings);
        final WatchExecutionContext ctx = WatcherTestUtils.createWatchExecutionContext();
        final MessagingException exception = expectThrows(
            MessagingException.class,
            () -> emailAction.execute("my_action_id", ctx, Payload.EMPTY)
        );
        final List<Throwable> allCauses = getAllCauses(exception);
        if (inFipsJvm()) {
            assertThat(
                allCauses.stream().map(c -> c.getClass().getCanonicalName()).collect(Collectors.toSet()),
                Matchers.hasItem("org.bouncycastle.tls.TlsFatalAlert")
            );
        } else {
            assertThat(allCauses, Matchers.hasItem(Matchers.instanceOf(SSLException.class)));
        }
    }

    public void testCanSendMessageToSmtpServerUsingTrustStore() throws Exception {
        assumeFalse("Can't use PKCS12 keystores in fips mode", inFipsJvm());
        List<MimeMessage> messages = new ArrayList<>();
        server.addListener(messages::add);
        try {
            final Settings.Builder settings = Settings.builder()
                .put("xpack.notification.email.ssl.truststore.path", getDataPath("test-smtp.p12"));
            final MockSecureSettings secureSettings = new MockSecureSettings();
            secureSettings.setString("xpack.notification.email.ssl.truststore.secure_password", "test-smtp");

            ExecutableEmailAction emailAction = buildEmailAction(settings, secureSettings);

            WatchExecutionContext ctx = WatcherTestUtils.createWatchExecutionContext();
            emailAction.execute("my_action_id", ctx, Payload.EMPTY);

            assertThat(messages, hasSize(1));
        } finally {
            server.clearListeners();
        }
    }

    public void testCanSendMessageToSmtpServerByDisablingVerification() throws Exception {
        assumeFalse("Can't run in a FIPS JVM with verification mode None", inFipsJvm());
        List<MimeMessage> messages = new ArrayList<>();
        server.addListener(messages::add);
        try {
            final Settings.Builder settings = Settings.builder().put("xpack.notification.email.ssl.verification_mode", "none");
            final MockSecureSettings secureSettings = new MockSecureSettings();
            ExecutableEmailAction emailAction = buildEmailAction(settings, secureSettings);

            WatchExecutionContext ctx = WatcherTestUtils.createWatchExecutionContext();
            emailAction.execute("my_action_id", ctx, Payload.EMPTY);

            assertThat(messages, hasSize(1));
        } finally {
            server.clearListeners();
        }
    }

    public void testCanSendMessageToSmtpServerUsingSmtpSslTrust() throws Exception {
        assumeFalse("Can't run in a FIPS JVM with verification mode None", inFipsJvm());
        List<MimeMessage> messages = new ArrayList<>();
        server.addListener(messages::add);
        try {
            final Settings.Builder settings = Settings.builder().put("xpack.notification.email.account.test.smtp.ssl.trust", "localhost");
            final MockSecureSettings secureSettings = new MockSecureSettings();
            ExecutableEmailAction emailAction = buildEmailAction(settings, secureSettings);

            WatchExecutionContext ctx = WatcherTestUtils.createWatchExecutionContext();
            emailAction.execute("my_action_id", ctx, Payload.EMPTY);

            assertThat(messages, hasSize(1));
        } finally {
            server.clearListeners();
        }
    }

    /**
     * This ordering could be considered to be backwards (the global "notification" settings take precedence
     * over the account level "smtp.ssl.trust" setting) but smtp.ssl.trust was ignored for a period of time (see #52153)
     * so this is the least breaking way to resolve that.
     */
    public void testNotificationSslSettingsOverrideSmtpSslTrust() throws Exception {
        List<MimeMessage> messages = new ArrayList<>();
        server.addListener(messages::add);
        try {
            final Settings.Builder settings = Settings.builder()
                .put("xpack.notification.email.account.test.smtp.ssl.trust", "localhost")
                .put("xpack.notification.email.ssl.verification_mode", "full");
            final MockSecureSettings secureSettings = new MockSecureSettings();
            ExecutableEmailAction emailAction = buildEmailAction(settings, secureSettings);

            WatchExecutionContext ctx = WatcherTestUtils.createWatchExecutionContext();
            final MessagingException exception = expectThrows(
                MessagingException.class,
                () -> emailAction.execute("my_action_id", ctx, Payload.EMPTY)
            );
            final List<Throwable> allCauses = getAllCauses(exception);
            if (inFipsJvm()) {
                assertThat(
                    allCauses.stream().map(c -> c.getClass().getCanonicalName()).collect(Collectors.toSet()),
                    Matchers.hasItem("org.bouncycastle.tls.TlsFatalAlert")
                );
            } else {
                assertThat(allCauses, Matchers.hasItem(Matchers.instanceOf(SSLException.class)));
            }

        } finally {
            server.clearListeners();
        }
    }

    private ExecutableEmailAction buildEmailAction(Settings.Builder baseSettings, MockSecureSettings secureSettings) {
        secureSettings.setString("xpack.notification.email.account.test.smtp.secure_password", EmailServer.PASSWORD);
        Settings settings = baseSettings.put("path.home", createTempDir())
            .put("xpack.notification.email.account.test.smtp.auth", true)
            .put("xpack.notification.email.account.test.smtp.user", EmailServer.USERNAME)
            .put("xpack.notification.email.account.test.smtp.port", server.port())
            .put("xpack.notification.email.account.test.smtp.host", "localhost")
            .setSecureSettings(secureSettings)
            .build();

        Set<Setting<?>> registeredSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        registeredSettings.addAll(EmailService.getSettings());
        ClusterSettings clusterSettings = new ClusterSettings(settings, registeredSettings);
        SSLService sslService = new SSLService(TestEnvironment.newEnvironment(settings));
        final EmailService emailService = new EmailService(settings, null, sslService, clusterSettings);
        EmailTemplate emailTemplate = EmailTemplate.builder()
            .from("from@example.org")
            .to("to@example.org")
            .subject("subject")
            .textBody("body")
            .build();
        final EmailAction emailAction = new EmailAction(emailTemplate, null, null, null, null, null);
        return new ExecutableEmailAction(emailAction, logger, emailService, textTemplateEngine, htmlSanitizer, Collections.emptyMap());
    }

    private List<Throwable> getAllCauses(Exception exception) {
        final List<Throwable> allCauses = new ArrayList<>();
        Throwable cause = exception.getCause();
        while (cause != null) {
            allCauses.add(cause);
            cause = cause.getCause();
        }
        return allCauses;
    }

}
