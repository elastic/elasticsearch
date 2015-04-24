/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.email.service;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.watcher.actions.email.service.support.EmailServer;
import org.elasticsearch.watcher.support.secret.Secret;
import org.elasticsearch.watcher.support.secret.SecretService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.*;

/**
 *
 */
public class AccountTests extends ElasticsearchTestCase {

    static final String USERNAME = "_user";
    static final String PASSWORD = "_passwd";

    private EmailServer server;

    @Before
    public void init() throws Exception {
        server = EmailServer.localhost("2500-2600", USERNAME, PASSWORD, logger);
    }

    @After
    public void cleanup() throws Exception {
        server.stop();
    }

    @Test
    public void testConfig() throws Exception {

        ImmutableSettings.Builder builder = ImmutableSettings.builder();

        Profile profile = rarely() ? Profile.STANDARD : randomFrom(Profile.values());
        if (profile != Profile.STANDARD) {
            builder.put("profile", profile.name());
        }

        Account.Config.EmailDefaults emailDefaults;
        if (randomBoolean()) {
            ImmutableSettings.Builder sb = ImmutableSettings.builder();
            if (randomBoolean()) {
                sb.put(Email.Field.FROM.getPreferredName(), "from@domain");
            }
            if (randomBoolean()) {
                sb.put(Email.Field.REPLY_TO.getPreferredName(), "replyto@domain");
            }
            if (randomBoolean()) {
                sb.put(Email.Field.PRIORITY.getPreferredName(), randomFrom(Email.Priority.values()));
            }
            if (randomBoolean()) {
                sb.put(Email.Field.TO.getPreferredName(), "to@domain");
            }
            if (randomBoolean()) {
                sb.put(Email.Field.CC.getPreferredName(), "cc@domain");
            }
            if (randomBoolean()) {
                sb.put(Email.Field.BCC.getPreferredName(), "bcc@domain");
            }
            if (randomBoolean()) {
                sb.put(Email.Field.SUBJECT.getPreferredName(), "_subject");
            }
            Settings settings = sb.build();
            emailDefaults = new Account.Config.EmailDefaults(settings);
            for (String name : settings.names()) {
                builder.put("email_defaults." + name, settings.get(name));
            }
        } else {
            emailDefaults = new Account.Config.EmailDefaults(ImmutableSettings.EMPTY);
        }

        Properties smtpProps = new Properties();
        ImmutableSettings.Builder smtpBuilder = ImmutableSettings.builder();
        String host = "somehost";
        String setting = randomFrom("host", "localaddress", "local_address");
        smtpBuilder.put(setting, host);
        if (setting.equals("local_address")) {
            // we need to remove the `_`... we only added support for `_` for readability
            // the actual properties (java mail properties) don't contain underscores
            setting = "localaddress";
        }
        smtpProps.put("mail.smtp." + setting, host);
        String user = null;
        if (randomBoolean()) {
            user = randomAsciiOfLength(5);
            setting = randomFrom("user", "from");
            smtpBuilder.put(setting, user);
            smtpProps.put("mail.smtp." + setting, user);
        }
        int port = 25;
        if (randomBoolean()) {
            port = randomIntBetween(2000, 2500);
            setting = randomFrom("port", "localport", "local_port");
            smtpBuilder.put(setting, port);
            if (setting.equals("local_port")) {
                setting = "localport";
            }
            smtpProps.setProperty("mail.smtp." + setting, String.valueOf(port));
        }
        String password = null;
        if (randomBoolean()) {
            password = randomAsciiOfLength(8);
            smtpBuilder.put("password", password);
            smtpProps.put("mail.smtp.password", password);
        }
        for (int i = 0; i < 5; i++) {
            String name = randomAsciiOfLength(5);
            String value = randomAsciiOfLength(6);
            smtpProps.put("mail.smtp." + name, value);
            smtpBuilder.put(name, value);
        }

        Settings smtpSettings = smtpBuilder.build();
        for (String name : smtpSettings.names()) {
            builder.put("smtp." + name, smtpSettings.get(name));
        }

        Settings settings = builder.build();

        Account.Config config = new Account.Config("_name", settings);

        assertThat(config.profile, is(profile));
        assertThat(config.defaults, equalTo(emailDefaults));
        assertThat(config.smtp, notNullValue());
        assertThat(config.smtp.port, is(port));
        assertThat(config.smtp.host, is(host));
        assertThat(config.smtp.user, is(user));
        if (password != null) {
            assertThat(config.smtp.password, is(password.toCharArray()));
        } else {
            assertThat(config.smtp.password, nullValue());
        }
        assertThat(config.smtp.properties, equalTo(smtpProps));
    }

    @Test
    public void testSend() throws Exception {
        Account account = new Account(new Account.Config("default", ImmutableSettings.builder()
                .put("smtp.host", "localhost")
                .put("smtp.port", server.port())
                .put("smtp.user", USERNAME)
                .put("smtp.password", PASSWORD)
                .build()), new SecretService.PlainText(), logger);

        Email email = Email.builder()
                .id("_id")
                .from(new Email.Address("from@domain.com"))
                .to(Email.AddressList.parse("To<to@domain.com>"))
                .subject("_subject")
                .textBody("_text_body")
                .build();

        final CountDownLatch latch = new CountDownLatch(1);
        EmailServer.Listener.Handle handle = server.addListener(new EmailServer.Listener() {
            @Override
            public void on(MimeMessage message) throws Exception {
                assertThat(message.getFrom().length, is(1));
                assertThat((InternetAddress) message.getFrom()[0], equalTo(new InternetAddress("from@domain.com")));
                assertThat(message.getRecipients(Message.RecipientType.TO).length, is(1));
                assertThat((InternetAddress) message.getRecipients(Message.RecipientType.TO)[0], equalTo(new InternetAddress("to@domain.com", "To")));
                assertThat(message.getSubject(), equalTo("_subject"));
                assertThat(Profile.STANDARD.textBody(message), equalTo("_text_body"));
                latch.countDown();
            }
        });

        account.send(email, null, Profile.STANDARD);

        if (!latch.await(5, TimeUnit.SECONDS)) {
            fail("waiting for email too long");
        }

        handle.remove();
    }

    @Test
    public void testSend_CC_BCC() throws Exception {
        Account account = new Account(new Account.Config("default", ImmutableSettings.builder()
                .put("smtp.host", "localhost")
                .put("smtp.port", server.port())
                .put("smtp.user", USERNAME)
                .put("smtp.password", PASSWORD)
                .build()), new SecretService.PlainText(), logger);

        Email email = Email.builder()
                .id("_id")
                .from(new Email.Address("from@domain.com"))
                .to(Email.AddressList.parse("TO<to@domain.com>"))
                .cc(Email.AddressList.parse("CC1<cc1@domain.com>,cc2@domain.com"))
                .bcc(Email.AddressList.parse("BCC1<bcc1@domain.com>,bcc2@domain.com"))
                .replyTo(Email.AddressList.parse("noreply@domain.com"))
                .build();

        final CountDownLatch latch = new CountDownLatch(5);
        EmailServer.Listener.Handle handle = server.addListener(new EmailServer.Listener() {
            @Override
            public void on(MimeMessage message) throws Exception {
                assertThat(message.getFrom().length, is(1));
                assertThat((InternetAddress) message.getFrom()[0], equalTo(new InternetAddress("from@domain.com")));
                assertThat(message.getRecipients(Message.RecipientType.TO).length, is(1));
                assertThat((InternetAddress) message.getRecipients(Message.RecipientType.TO)[0], equalTo(new InternetAddress("to@domain.com", "TO")));
                assertThat(message.getRecipients(Message.RecipientType.CC).length, is(2));
                assertThat(message.getRecipients(Message.RecipientType.CC), hasItemInArray((Address) new InternetAddress("cc1@domain.com", "CC1")));
                assertThat(message.getRecipients(Message.RecipientType.CC), hasItemInArray((Address) new InternetAddress("cc2@domain.com")));
                assertThat(message.getReplyTo(), arrayWithSize(1));
                assertThat(message.getReplyTo(), hasItemInArray((Address) new InternetAddress("noreply@domain.com")));
                // bcc should not be there... (it's bcc after all)
                latch.countDown();
            }
        });

        account.send(email, null, Profile.STANDARD);

        if (!latch.await(5, TimeUnit.SECONDS)) {
            fail("waiting for email too long");
        }

        handle.remove();
    }

    @Test
    public void testSend_Authentication() throws Exception {
        Account account = new Account(new Account.Config("default", ImmutableSettings.builder()
                .put("smtp.host", "localhost")
                .put("smtp.port", server.port())
                .build()), new SecretService.PlainText(), logger);

        Email email = Email.builder()
                .id("_id")
                .from(new Email.Address("from@domain.com"))
                .to(Email.AddressList.parse("To<to@domain.com>"))
                .subject("_subject")
                .textBody("_text_body")
                .build();

        final CountDownLatch latch = new CountDownLatch(1);
        EmailServer.Listener.Handle handle = server.addListener(new EmailServer.Listener() {
            @Override
            public void on(MimeMessage message) throws Exception {
                latch.countDown();
            }
        });

        account.send(email, new Authentication(USERNAME, new Secret(PASSWORD.toCharArray())), Profile.STANDARD);

        if (!latch.await(5, TimeUnit.SECONDS)) {
            fail("waiting for email too long");
        }

        handle.remove();
    }

}
