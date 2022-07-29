/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.actions.email;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
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
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.mail.internet.MimeMessage;

import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;

public class EmailMessageIdTests extends ESTestCase {

    private EmailServer server;
    private TextTemplateEngine textTemplateEngine = new MockTextTemplateEngine();
    private HtmlSanitizer htmlSanitizer = new HtmlSanitizer(Settings.EMPTY);
    private EmailService emailService;
    private EmailAction emailAction;

    @Before
    public void startSmtpServer() {
        server = EmailServer.localhost(logger);

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.notification.email.account.test.smtp.secure_password", EmailServer.PASSWORD);
        Settings settings = Settings.builder()
            .put("xpack.notification.email.account.test.smtp.auth", true)
            .put("xpack.notification.email.account.test.smtp.user", EmailServer.USERNAME)
            .put("xpack.notification.email.account.test.smtp.port", server.port())
            .put("xpack.notification.email.account.test.smtp.host", "localhost")
            .setSecureSettings(secureSettings)
            .build();

        Set<Setting<?>> registeredSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        registeredSettings.addAll(EmailService.getSettings());
        ClusterSettings clusterSettings = new ClusterSettings(settings, registeredSettings);
        emailService = new EmailService(settings, null, mock(SSLService.class), clusterSettings);
        EmailTemplate emailTemplate = EmailTemplate.builder()
            .from("from@example.org")
            .to("to@example.org")
            .subject("subject")
            .textBody("body")
            .build();
        emailAction = new EmailAction(emailTemplate, null, null, null, null, null);
    }

    @After
    public void stopSmtpServer() {
        server.stop();
    }

    public void testThatMessageIdIsUnique() throws Exception {
        List<MimeMessage> messages = new ArrayList<>();
        server.addListener(messages::add);
        ExecutableEmailAction firstEmailAction = new ExecutableEmailAction(
            emailAction,
            logger,
            emailService,
            textTemplateEngine,
            htmlSanitizer,
            Collections.emptyMap()
        );
        ExecutableEmailAction secondEmailAction = new ExecutableEmailAction(
            emailAction,
            logger,
            emailService,
            textTemplateEngine,
            htmlSanitizer,
            Collections.emptyMap()
        );

        WatchExecutionContext ctx = WatcherTestUtils.createWatchExecutionContext();
        firstEmailAction.execute("my_first_action_id", ctx, Payload.EMPTY);
        secondEmailAction.execute("my_second_action_id", ctx, Payload.EMPTY);

        assertThat(messages, hasSize(2));
        // check for unique message ids, should be two as well
        Set<String> messageIds = new HashSet<>();
        for (MimeMessage message : messages) {
            messageIds.add(message.getMessageID());
        }
        assertThat(messageIds, hasSize(2));
    }
}
