/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification.email;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.watcher.common.secret.Secret;
import org.junit.Before;

import java.util.HashSet;
import java.util.Properties;

import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EmailServiceTests extends ESTestCase {
    private EmailService service;
    private Account account;

    @Before
    public void init() throws Exception {
        account = mock(Account.class);
        service = new EmailService(Settings.builder().put("xpack.notification.email.account.account1.foo", "bar").build(), null,
            mock(SSLService.class), new ClusterSettings(Settings.EMPTY, new HashSet<>(EmailService.getSettings()))) {
            @Override
            protected Account createAccount(String name, Settings accountSettings) {
                return account;
            }
        };
    }

    public void testSend() throws Exception {
        when(account.name()).thenReturn("account1");
        Email email = mock(Email.class);
        Authentication auth = new Authentication("user", new Secret("passwd".toCharArray()));
        Profile profile = randomFrom(Profile.values());
        when(account.send(email, auth, profile)).thenReturn(email);
        EmailService.EmailSent sent = service.send(email, auth, profile, "account1");
        verify(account).send(email, auth, profile);
        assertThat(sent, notNullValue());
        assertThat(sent.email(), sameInstance(email));
        assertThat(sent.account(), is("account1"));
    }

    public void testAccountSmtpPropertyConfiguration() {
        Settings settings = Settings.builder()
                .put("xpack.notification.email.account.account1.smtp.host", "localhost")
                .put("xpack.notification.email.account.account1.smtp.starttls.required", "true")
                .put("xpack.notification.email.account.account2.smtp.host", "localhost")
                .put("xpack.notification.email.account.account2.smtp.connection_timeout", "1m")
                .put("xpack.notification.email.account.account2.smtp.timeout", "1m")
                .put("xpack.notification.email.account.account2.smtp.write_timeout", "1m")
                .put("xpack.notification.email.account.account3.smtp.host", "localhost")
                .put("xpack.notification.email.account.account3.smtp.send_partial", true)
                .put("xpack.notification.email.account.account4.smtp.host", "localhost")
                .put("xpack.notification.email.account.account4.smtp.local_address", "localhost")
                .put("xpack.notification.email.account.account4.smtp.local_port", "1025")
                .put("xpack.notification.email.account.account5.smtp.host", "localhost")
                .put("xpack.notification.email.account.account5.smtp.wait_on_quit", true)
                .put("xpack.notification.email.account.account5.smtp.ssl.trust", "host1,host2,host3")
                .build();
        EmailService emailService = new EmailService(settings, null, mock(SSLService.class),
                new ClusterSettings(Settings.EMPTY, new HashSet<>(EmailService.getSettings())));

        Account account1 = emailService.getAccount("account1");
        Properties properties1 = account1.getConfig().smtp.properties;
        assertThat(properties1, hasEntry("mail.smtp.starttls.required", "true"));
        assertThat(properties1, hasEntry("mail.smtp.connectiontimeout", "120000"));
        assertThat(properties1, hasEntry("mail.smtp.writetimeout", "120000"));
        assertThat(properties1, hasEntry("mail.smtp.timeout", "120000"));
        assertThat(properties1, not(hasKey("mail.smtp.sendpartial")));
        assertThat(properties1, not(hasKey("mail.smtp.waitonquit")));
        assertThat(properties1, not(hasKey("mail.smtp.localport")));

        Account account2 = emailService.getAccount("account2");
        Properties properties2 = account2.getConfig().smtp.properties;
        assertThat(properties2, hasEntry("mail.smtp.connectiontimeout", "60000"));
        assertThat(properties2, hasEntry("mail.smtp.writetimeout", "60000"));
        assertThat(properties2, hasEntry("mail.smtp.timeout", "60000"));

        Account account3 = emailService.getAccount("account3");
        Properties properties3 = account3.getConfig().smtp.properties;
        assertThat(properties3, hasEntry("mail.smtp.sendpartial", "true"));

        Account account4 = emailService.getAccount("account4");
        Properties properties4 = account4.getConfig().smtp.properties;
        assertThat(properties4, hasEntry("mail.smtp.localaddress", "localhost"));
        assertThat(properties4, hasEntry("mail.smtp.localport", "1025"));

        Account account5 = emailService.getAccount("account5");
        Properties properties5 = account5.getConfig().smtp.properties;
        assertThat(properties5, hasEntry("mail.smtp.quitwait", "true"));
        assertThat(properties5, hasEntry("mail.smtp.ssl.trust", "host1,host2,host3"));
    }
}
