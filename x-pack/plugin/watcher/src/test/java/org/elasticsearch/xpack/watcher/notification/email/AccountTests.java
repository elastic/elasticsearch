/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.email;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.common.secret.Secret;
import org.elasticsearch.xpack.watcher.notification.email.support.EmailServer;
import org.junit.After;
import org.junit.Before;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.internet.InternetAddress;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AccountTests extends ESTestCase {

    private EmailServer server;

    @Before
    public void init() throws Exception {
        server = EmailServer.localhost(logger);
    }

    @After
    public void cleanup() throws Exception {
        server.stop();
    }

    public void testConfig() throws Exception {
        String accountName = "_name";

        Settings.Builder builder = Settings.builder();

        Profile profile = rarely() ? Profile.STANDARD : randomFrom(Profile.values());
        if (profile != Profile.STANDARD) {
            builder.put("profile", profile.name());
        }

        Account.Config.EmailDefaults emailDefaults;
        if (randomBoolean()) {
            Settings.Builder sb = Settings.builder();
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
            emailDefaults = new Account.Config.EmailDefaults(accountName, settings);
            for (String name : settings.names()) {
                builder.put("email_defaults." + name, settings.get(name));
            }
        } else {
            emailDefaults = new Account.Config.EmailDefaults(accountName, Settings.EMPTY);
        }

        Properties smtpProps = new Properties();
        Settings.Builder smtpBuilder = Settings.builder();
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
            user = randomAlphaOfLength(5);
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
            password = randomAlphaOfLength(8);
            final MockSecureSettings secureSettings = new MockSecureSettings();
            secureSettings.setString("smtp." + Account.SECURE_PASSWORD_SETTING.getKey(), password);
            builder.setSecureSettings(secureSettings);
        }
        for (int i = 0; i < 5; i++) {
            String name = randomAlphaOfLength(5);
            String value = randomAlphaOfLength(6);
            smtpProps.put("mail.smtp." + name, value);
            smtpBuilder.put(name, value);
        }

        // default properties
        for (String name : new String[] { "connection_timeout", "write_timeout", "timeout" }) {
            String propertyName = name.replaceAll("_", "");
            smtpProps.put(
                "mail.smtp." + propertyName,
                String.valueOf(TimeValue.parseTimeValue(Account.DEFAULT_SMTP_TIMEOUT_SETTINGS.get(name), name).millis())
            );
        }

        Settings smtpSettings = smtpBuilder.build();
        for (String name : smtpSettings.names()) {
            builder.put("smtp." + name, smtpSettings.get(name));
        }

        Settings settings = builder.build();

        Account.Config config = new Account.Config(accountName, settings, null, logger);

        assertThat(config.profile, is(profile));
        assertThat(config.defaults, equalTo(emailDefaults));
        assertThat(config.smtp, notNullValue());
        assertThat(config.smtp.port, is(port));
        assertThat(config.smtp.host, is(host));
        assertThat(config.smtp.user, is(user));
        if (password != null) {
            assertThat(config.smtp.password.getChars(), is(password.toCharArray()));
        } else {
            assertThat(config.smtp.password, nullValue());
        }
        assertThat(config.smtp.properties, equalTo(smtpProps));
    }

    public void testSend() throws Exception {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("smtp." + Account.SECURE_PASSWORD_SETTING.getKey(), EmailServer.PASSWORD);
        Account account = new Account(
            new Account.Config(
                "default",
                Settings.builder()
                    .put("smtp.host", "localhost")
                    .put("smtp.port", server.port())
                    .put("smtp.user", EmailServer.USERNAME)
                    .setSecureSettings(secureSettings)
                    .build(),
                null,
                logger
            ),
            null,
            logger
        );

        Email email = Email.builder()
            .id("_id")
            .from(new Email.Address("from@domain.com"))
            .to(Email.AddressList.parse("To<to@domain.com>"))
            .subject("_subject")
            .textBody("_text_body")
            .build();

        final CountDownLatch latch = new CountDownLatch(1);
        server.addListener(message -> {
            assertThat(message.getFrom().length, is(1));
            assertThat(message.getFrom()[0], equalTo(new InternetAddress("from@domain.com")));
            assertThat(message.getRecipients(Message.RecipientType.TO).length, is(1));
            assertThat(message.getRecipients(Message.RecipientType.TO)[0], equalTo(new InternetAddress("to@domain.com", "To")));
            assertThat(message.getSubject(), equalTo("_subject"));
            assertThat(Profile.STANDARD.textBody(message), equalTo("_text_body"));
            latch.countDown();
        });

        account.send(email, null, Profile.STANDARD);

        if (latch.await(5, TimeUnit.SECONDS) == false) {
            fail("waiting for email too long");
        }
    }

    public void testSendCCAndBCC() throws Exception {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("smtp." + Account.SECURE_PASSWORD_SETTING.getKey(), EmailServer.PASSWORD);
        Account account = new Account(
            new Account.Config(
                "default",
                Settings.builder()
                    .put("smtp.host", "localhost")
                    .put("smtp.port", server.port())
                    .put("smtp.user", EmailServer.USERNAME)
                    .setSecureSettings(secureSettings)
                    .build(),
                null,
                logger
            ),
            null,
            logger
        );

        Email email = Email.builder()
            .id("_id")
            .from(new Email.Address("from@domain.com"))
            .to(Email.AddressList.parse("TO<to@domain.com>"))
            .cc(Email.AddressList.parse("CC1<cc1@domain.com>,cc2@domain.com"))
            .bcc(Email.AddressList.parse("BCC1<bcc1@domain.com>,bcc2@domain.com"))
            .replyTo(Email.AddressList.parse("noreply@domain.com"))
            .build();

        final CountDownLatch latch = new CountDownLatch(5);
        server.addListener(message -> {
            assertThat(message.getFrom().length, is(1));
            assertThat(message.getFrom()[0], equalTo(new InternetAddress("from@domain.com")));
            assertThat(message.getRecipients(Message.RecipientType.TO).length, is(1));
            assertThat(message.getRecipients(Message.RecipientType.TO)[0], equalTo(new InternetAddress("to@domain.com", "TO")));
            assertThat(message.getRecipients(Message.RecipientType.CC).length, is(2));
            assertThat(
                message.getRecipients(Message.RecipientType.CC),
                hasItemInArray((Address) new InternetAddress("cc1@domain.com", "CC1"))
            );
            assertThat(message.getRecipients(Message.RecipientType.CC), hasItemInArray((Address) new InternetAddress("cc2@domain.com")));
            assertThat(message.getReplyTo(), arrayWithSize(1));
            assertThat(message.getReplyTo(), hasItemInArray((Address) new InternetAddress("noreply@domain.com")));
            // bcc should not be there... (it's bcc after all)
            latch.countDown();
        });

        account.send(email, null, Profile.STANDARD);

        if (latch.await(5, TimeUnit.SECONDS) == false) {
            fail("waiting for email too long");
        }
    }

    public void testSendAuthentication() throws Exception {
        Account account = new Account(
            new Account.Config(
                "default",
                Settings.builder().put("smtp.host", "localhost").put("smtp.port", server.port()).build(),
                null,
                logger
            ),
            null,
            logger
        );

        Email email = Email.builder()
            .id("_id")
            .from(new Email.Address("from@domain.com"))
            .to(Email.AddressList.parse("To<to@domain.com>"))
            .subject("_subject")
            .textBody("_text_body")
            .build();

        final CountDownLatch latch = new CountDownLatch(1);
        server.addListener(message -> latch.countDown());

        account.send(email, new Authentication(EmailServer.USERNAME, new Secret(EmailServer.PASSWORD.toCharArray())), Profile.STANDARD);

        if (latch.await(5, TimeUnit.SECONDS) == false) {
            fail("waiting for email too long");
        }
    }

    public void testDefaultAccountTimeout() {
        Account account = new Account(
            new Account.Config(
                "default",
                Settings.builder().put("smtp.host", "localhost").put("smtp.port", server.port()).build(),
                null,
                logger
            ),
            null,
            logger
        );

        Properties mailProperties = account.getConfig().smtp.properties;
        assertThat(mailProperties.get("mail.smtp.connectiontimeout"), is(String.valueOf(TimeValue.timeValueMinutes(2).millis())));
        assertThat(mailProperties.get("mail.smtp.writetimeout"), is(String.valueOf(TimeValue.timeValueMinutes(2).millis())));
        assertThat(mailProperties.get("mail.smtp.timeout"), is(String.valueOf(TimeValue.timeValueMinutes(2).millis())));
    }

    public void testAccountTimeoutsCanBeConfigureAsTimeValue() {
        Account account = new Account(
            new Account.Config(
                "default",
                Settings.builder()
                    .put("smtp.host", "localhost")
                    .put("smtp.port", server.port())
                    .put("smtp.connection_timeout", TimeValue.timeValueMinutes(4))
                    .put("smtp.write_timeout", TimeValue.timeValueMinutes(6))
                    .put("smtp.timeout", TimeValue.timeValueMinutes(8))
                    .build(),
                null,
                logger
            ),
            null,
            logger
        );

        Properties mailProperties = account.getConfig().smtp.properties;

        assertThat(mailProperties.get("mail.smtp.connectiontimeout"), is(String.valueOf(TimeValue.timeValueMinutes(4).millis())));
        assertThat(mailProperties.get("mail.smtp.writetimeout"), is(String.valueOf(TimeValue.timeValueMinutes(6).millis())));
        assertThat(mailProperties.get("mail.smtp.timeout"), is(String.valueOf(TimeValue.timeValueMinutes(8).millis())));
    }

    public void testAccountTimeoutsConfiguredAsNumberAreRejected() {
        expectThrows(IllegalArgumentException.class, () -> {
            new Account(
                new Account.Config(
                    "default",
                    Settings.builder()
                        .put("smtp.host", "localhost")
                        .put("smtp.port", server.port())
                        .put("smtp.connection_timeout", 4000)
                        .build(),
                    null,
                    logger
                ),
                null,
                logger
            );
        });
    }

}
