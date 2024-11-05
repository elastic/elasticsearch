/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.email;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.watcher.common.secret.Secret;
import org.junit.Before;

import java.io.UnsupportedEncodingException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.mail.MessagingException;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
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
        service = new EmailService(
            Settings.builder().put("xpack.notification.email.account.account1.foo", "bar").build(),
            null,
            mock(SSLService.class),
            new ClusterSettings(Settings.EMPTY, new HashSet<>(EmailService.getSettings()))
        ) {
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

    public void testDomainAndRecipientAllowCantBeSetAtSameTime() {
        Settings settings = Settings.builder()
            .putList("xpack.notification.email.account.domain_allowlist", "bar.com")
            .putList("xpack.notification.email.recipient_allowlist", "*-user@potato.com")
            .build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new EmailService(
                settings,
                null,
                mock(SSLService.class),
                new ClusterSettings(Settings.EMPTY, new HashSet<>(EmailService.getSettings()))
            )
        );

        assertThat(
            e.getMessage(),
            containsString(
                "Cannot set both [xpack.notification.email.recipient_allowlist] and "
                    + "[xpack.notification.email.account.domain_allowlist] to a non [\"*\"] value at the same time."
            )
        );
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
        EmailService emailService = new EmailService(
            settings,
            null,
            mock(SSLService.class),
            new ClusterSettings(Settings.EMPTY, new HashSet<>(EmailService.getSettings()))
        );

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

    public void testExtractDomains() throws Exception {
        Email email = new Email(
            "id",
            new Email.Address("foo@bar.com", "foo@bar.com"),
            createAddressList("foo@bar.com", "baz@eggplant.com"),
            randomFrom(Email.Priority.values()),
            ZonedDateTime.now(),
            createAddressList("foo@bar.com", "bar@eggplant.com", "person@example.com"),
            createAddressList("me@another.com", "other@bar.com"),
            createAddressList("onemore@bar.com", "private@bcc.com"),
            "subject",
            "body",
            "htmlbody",
            Collections.emptyMap()
        );
        assertThat(
            EmailService.getRecipients(email, true),
            containsInAnyOrder("bar.com", "eggplant.com", "example.com", "another.com", "bcc.com")
        );

        email = new Email(
            "id",
            new Email.Address("foo@bar.com", "foo@bar.com"),
            createAddressList("foo@bar.com", "baz@potato.com"),
            randomFrom(Email.Priority.values()),
            ZonedDateTime.now(),
            createAddressList("foo@bar.com", "bar@eggplant.com", "person@example.com"),
            null,
            null,
            "subject",
            "body",
            "htmlbody",
            Collections.emptyMap()
        );
        assertThat(EmailService.getRecipients(email, true), containsInAnyOrder("bar.com", "eggplant.com", "example.com"));
    }

    public void testAllowedDomain() throws Exception {
        Email email = new Email(
            "id",
            new Email.Address("foo@bar.com", "Mr. Foo Man"),
            createAddressList("foo@bar.com", "baz@potato.com"),
            randomFrom(Email.Priority.values()),
            ZonedDateTime.now(),
            createAddressList("foo@bar.com"),
            null,
            null,
            "subject",
            "body",
            "htmlbody",
            Collections.emptyMap()
        );
        assertTrue(EmailService.recipientDomainsInAllowList(email, Set.of("*")));
        assertFalse(EmailService.recipientDomainsInAllowList(email, Set.of()));
        assertFalse(EmailService.recipientDomainsInAllowList(email, Set.of("")));
        assertTrue(EmailService.recipientDomainsInAllowList(email, Set.of("other.com", "bar.com")));
        assertTrue(EmailService.recipientDomainsInAllowList(email, Set.of("other.com", "*.com")));
        assertTrue(EmailService.recipientDomainsInAllowList(email, Set.of("*.CoM")));

        // Invalid email in CC doesn't blow up
        email = new Email(
            "id",
            new Email.Address("foo@bar.com", "Mr. Foo Man"),
            createAddressList("foo@bar.com", "baz@potato.com"),
            randomFrom(Email.Priority.values()),
            ZonedDateTime.now(),
            createAddressList("foo@bar.com"),
            createAddressList("badEmail"),
            null,
            "subject",
            "body",
            "htmlbody",
            Collections.emptyMap()
        );
        assertFalse(EmailService.recipientDomainsInAllowList(email, Set.of("other.com", "bar.com")));

        // Check CC
        email = new Email(
            "id",
            new Email.Address("foo@bar.com", "Mr. Foo Man"),
            createAddressList("foo@bar.com", "baz@potato.com"),
            randomFrom(Email.Priority.values()),
            ZonedDateTime.now(),
            createAddressList("foo@bar.com"),
            createAddressList("thing@other.com"),
            null,
            "subject",
            "body",
            "htmlbody",
            Collections.emptyMap()
        );
        assertTrue(EmailService.recipientDomainsInAllowList(email, Set.of("other.com", "bar.com")));
        assertFalse(EmailService.recipientDomainsInAllowList(email, Set.of("bar.com")));

        // Check BCC
        email = new Email(
            "id",
            new Email.Address("foo@bar.com", "Mr. Foo Man"),
            createAddressList("foo@bar.com", "baz@potato.com"),
            randomFrom(Email.Priority.values()),
            ZonedDateTime.now(),
            createAddressList("foo@bar.com"),
            null,
            createAddressList("thing@other.com"),
            "subject",
            "body",
            "htmlbody",
            Collections.emptyMap()
        );
        assertTrue(EmailService.recipientDomainsInAllowList(email, Set.of("other.com", "bar.com")));
        assertFalse(EmailService.recipientDomainsInAllowList(email, Set.of("bar.com")));
    }

    public void testSendEmailWithDomainNotInAllowList() throws Exception {
        service.updateAllowedDomains(Collections.singletonList(randomFrom("bar.*", "bar.com", "b*")));
        Email email = new Email(
            "id",
            new Email.Address("foo@bar.com", "Mr. Foo Man"),
            createAddressList("foo@bar.com", "baz@potato.com"),
            randomFrom(Email.Priority.values()),
            ZonedDateTime.now(),
            createAddressList("foo@bar.com", "non-whitelisted@invalid.com"),
            null,
            null,
            "subject",
            "body",
            "htmlbody",
            Collections.emptyMap()
        );
        when(account.name()).thenReturn("account1");
        Authentication auth = new Authentication("user", new Secret("passwd".toCharArray()));
        Profile profile = randomFrom(Profile.values());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> service.send(email, auth, profile, "account1"));
        assertThat(
            e.getMessage(),
            containsString(
                "failed to send email with subject [subject] and recipient domains "
                    + "[bar.com, invalid.com], one or more recipients is not specified in the domain allow list setting "
                    + "[xpack.notification.email.account.domain_allowlist]."
            )
        );
    }

    public void testChangeDomainAllowListSetting() throws UnsupportedEncodingException, MessagingException {
        Settings settings = Settings.builder()
            .put("xpack.notification.email.account.account1.foo", "bar")
            // Setting a random SMTP server name and an invalid port so that sending emails is guaranteed to fail:
            .put("xpack.notification.email.account.account1.smtp.host", randomAlphaOfLength(10))
            .put("xpack.notification.email.account.account1.smtp.port", -100)
            .putList("xpack.notification.email.account.domain_allowlist", "bar.com")
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet<>(EmailService.getSettings()));
        EmailService emailService = new EmailService(settings, null, mock(SSLService.class), clusterSettings);
        Email email = new Email(
            "id",
            new Email.Address("foo@bar.com", "Mr. Foo Man"),
            createAddressList("foo@bar.com", "baz@potato.com"),
            randomFrom(Email.Priority.values()),
            ZonedDateTime.now(),
            createAddressList("foo@bar.com", "non-whitelisted@invalid.com"),
            null,
            null,
            "subject",
            "body",
            "htmlbody",
            Collections.emptyMap()
        );
        when(account.name()).thenReturn("account1");
        Authentication auth = new Authentication("user", new Secret("passwd".toCharArray()));
        Profile profile = randomFrom(Profile.values());

        // This send will fail because one of the recipients ("non-whitelisted@invalid.com") is in a domain that is not in the allowed list
        IllegalArgumentException e1 = expectThrows(
            IllegalArgumentException.class,
            () -> emailService.send(email, auth, profile, "account1")
        );
        assertThat(
            e1.getMessage(),
            containsString(
                "failed to send email with subject [subject] and recipient domains "
                    + "[bar.com, invalid.com], one or more recipients is not specified in the domain allow list setting "
                    + "[xpack.notification.email.account.domain_allowlist]."
            )
        );

        // Now dynamically add "invalid.com" to the list of allowed domains:
        Settings newSettings = Settings.builder()
            .putList("xpack.notification.email.account.domain_allowlist", "bar.com", "invalid.com")
            .build();
        clusterSettings.applySettings(newSettings);
        // Still expect an exception because we're not actually sending the email, but it's no longer because the domain isn't allowed:
        IllegalArgumentException e2 = expectThrows(
            IllegalArgumentException.class,
            () -> emailService.send(email, auth, profile, "account1")
        );
        assertThat(e2.getMessage(), containsString("port out of range"));
    }

    public void testRecipientAddressInAllowList_EmptyAllowedPatterns() throws UnsupportedEncodingException {
        Email email = createTestEmail("foo@bar.com", "baz@potato.com");
        Set<String> allowedPatterns = Set.of();
        assertThat(EmailService.recipientAddressInAllowList(email, allowedPatterns), is(false));
    }

    public void testRecipientAddressInAllowList_WildcardPattern() throws UnsupportedEncodingException {
        Email email = createTestEmail("foo@bar.com", "baz@potato.com");
        Set<String> allowedPatterns = Set.of("*");
        assertThat(EmailService.recipientAddressInAllowList(email, allowedPatterns), is(true));
    }

    public void testRecipientAddressInAllowList_SpecificPattern() throws UnsupportedEncodingException {
        Email email = createTestEmail("foo@bar.com", "baz@potato.com");
        Set<String> allowedPatterns = Set.of("foo@bar.com");
        assertThat(EmailService.recipientAddressInAllowList(email, allowedPatterns), is(false));
    }

    public void testRecipientAddressInAllowList_MultiplePatterns() throws UnsupportedEncodingException {
        Email email = createTestEmail("foo@bar.com", "baz@potato.com");
        Set<String> allowedPatterns = Set.of("foo@bar.com", "baz@potato.com");
        assertThat(EmailService.recipientAddressInAllowList(email, allowedPatterns), is(true));
    }

    public void testRecipientAddressInAllowList_MixedCasePatterns() throws UnsupportedEncodingException {
        Email email = createTestEmail("foo@bar.com", "baz@potato.com");
        Set<String> allowedPatterns = Set.of("FOO@BAR.COM", "BAZ@POTATO.COM");
        assertThat(EmailService.recipientAddressInAllowList(email, allowedPatterns), is(true));
    }

    public void testRecipientAddressInAllowList_PartialWildcardPrefixPattern() throws UnsupportedEncodingException {
        Email email = createTestEmail("foo@bar.com", "baz@potato.com");
        Set<String> allowedPatterns = Set.of("foo@*", "baz@*");
        assertThat(EmailService.recipientAddressInAllowList(email, allowedPatterns), is(true));
    }

    public void testRecipientAddressInAllowList_PartialWildcardSuffixPattern() throws UnsupportedEncodingException {
        Email email = createTestEmail("foo@bar.com", "baz@potato.com");
        Set<String> allowedPatterns = Set.of("*@bar.com", "*@potato.com");
        assertThat(EmailService.recipientAddressInAllowList(email, allowedPatterns), is(true));
    }

    public void testRecipientAddressInAllowList_DisallowedCCAddressesFails() throws UnsupportedEncodingException {
        Email email = new Email(
            "id",
            new Email.Address("sender@domain.com", "Sender"),
            createAddressList("foo@bar.com"),
            randomFrom(Email.Priority.values()),
            ZonedDateTime.now(),
            createAddressList("foo@bar.com"),
            createAddressList("cc@allowed.com", "cc@notallowed.com"),
            null,
            "subject",
            "body",
            "htmlbody",
            Collections.emptyMap()
        );
        Set<String> allowedPatterns = Set.of("foo@bar.com", "cc@allowed.com");
        assertThat(EmailService.recipientAddressInAllowList(email, allowedPatterns), is(false));
    }

    public void testRecipientAddressInAllowList_DisallowedBCCAddressesFails() throws UnsupportedEncodingException {
        Email email = new Email(
            "id",
            new Email.Address("sender@domain.com", "Sender"),
            createAddressList("foo@bar.com"),
            randomFrom(Email.Priority.values()),
            ZonedDateTime.now(),
            createAddressList("foo@bar.com"),
            null,
            createAddressList("bcc@allowed.com", "bcc@notallowed.com"),
            "subject",
            "body",
            "htmlbody",
            Collections.emptyMap()
        );
        Set<String> allowedPatterns = Set.of("foo@bar.com", "bcc@allowed.com");
        assertThat(EmailService.recipientAddressInAllowList(email, allowedPatterns), is(false));
    }

    public void testAllowedRecipient() throws Exception {
        Email email = new Email(
            "id",
            new Email.Address("foo@bar.com", "Mr. Foo Man"),
            createAddressList("foo@bar.com", "baz@potato.com"),
            randomFrom(Email.Priority.values()),
            ZonedDateTime.now(),
            createAddressList("foo@bar.com"),
            null,
            null,
            "subject",
            "body",
            "htmlbody",
            Collections.emptyMap()
        );
        assertTrue(EmailService.recipientAddressInAllowList(email, Set.of("*")));
        assertFalse(EmailService.recipientAddressInAllowList(email, Set.of()));
        assertFalse(EmailService.recipientAddressInAllowList(email, Set.of("")));
        assertTrue(EmailService.recipientAddressInAllowList(email, Set.of("foo@other.com", "*o@bar.com")));
        assertTrue(EmailService.recipientAddressInAllowList(email, Set.of("buzz@other.com", "*.com")));
        assertTrue(EmailService.recipientAddressInAllowList(email, Set.of("*.CoM")));

        // Invalid email in CC doesn't blow up
        email = new Email(
            "id",
            new Email.Address("foo@bar.com", "Mr. Foo Man"),
            createAddressList("foo@bar.com", "baz@potato.com"),
            randomFrom(Email.Priority.values()),
            ZonedDateTime.now(),
            createAddressList("foo@bar.com"),
            createAddressList("badEmail"),
            null,
            "subject",
            "body",
            "htmlbody",
            Collections.emptyMap()
        );
        assertFalse(EmailService.recipientAddressInAllowList(email, Set.of("*@other.com", "*iii@bar.com")));

        // Check CC
        email = new Email(
            "id",
            new Email.Address("foo@bar.com", "Mr. Foo Man"),
            createAddressList("foo@bar.com", "baz@potato.com"),
            randomFrom(Email.Priority.values()),
            ZonedDateTime.now(),
            createAddressList("foo@bar.com"),
            createAddressList("thing@other.com"),
            null,
            "subject",
            "body",
            "htmlbody",
            Collections.emptyMap()
        );
        assertTrue(EmailService.recipientAddressInAllowList(email, Set.of("*@other.com", "*@bar.com")));
        assertFalse(EmailService.recipientAddressInAllowList(email, Set.of("*oo@bar.com")));

        // Check BCC
        email = new Email(
            "id",
            new Email.Address("foo@bar.com", "Mr. Foo Man"),
            createAddressList("foo@bar.com", "baz@potato.com"),
            randomFrom(Email.Priority.values()),
            ZonedDateTime.now(),
            createAddressList("foo@bar.com"),
            null,
            createAddressList("thing@other.com"),
            "subject",
            "body",
            "htmlbody",
            Collections.emptyMap()
        );
        assertTrue(EmailService.recipientAddressInAllowList(email, Set.of("*@other.com", "*@bar.com")));
        assertFalse(EmailService.recipientAddressInAllowList(email, Set.of("*oo@bar.com")));
    }

    public void testSendEmailWithRecipientNotInAllowList() throws Exception {
        service.updateAllowedRecipientPatterns(Collections.singletonList(randomFrom("*@bar.*", "*@bar.com", "*b*")));
        Email email = new Email(
            "id",
            new Email.Address("foo@bar.com", "Mr. Foo Man"),
            createAddressList("foo@bar.com", "baz@potato.com"),
            randomFrom(Email.Priority.values()),
            ZonedDateTime.now(),
            createAddressList("foo@bar.com", "non-whitelisted@invalid.com"),
            null,
            null,
            "subject",
            "body",
            "htmlbody",
            Collections.emptyMap()
        );
        when(account.name()).thenReturn("account1");
        Authentication auth = new Authentication("user", new Secret("passwd".toCharArray()));
        Profile profile = randomFrom(Profile.values());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> service.send(email, auth, profile, "account1"));
        assertThat(
            e.getMessage(),
            containsString(
                "failed to send email with subject [subject] and recipients [non-whitelisted@invalid.com, foo@bar.com], "
                    + "one or more recipients is not specified in the domain allow list setting "
                    + "[xpack.notification.email.recipient_allowlist]."
            )
        );
    }

    public void testChangeRecipientAllowListSetting() throws UnsupportedEncodingException, MessagingException {
        Settings settings = Settings.builder()
            .put("xpack.notification.email.account.account1.foo", "bar")
            // Setting a random SMTP server name and an invalid port so that sending emails is guaranteed to fail:
            .put("xpack.notification.email.account.account1.smtp.host", randomAlphaOfLength(10))
            .put("xpack.notification.email.account.account1.smtp.port", -100)
            .putList("xpack.notification.email.recipient_allowlist", "*oo@bar.com")
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet<>(EmailService.getSettings()));
        EmailService emailService = new EmailService(settings, null, mock(SSLService.class), clusterSettings);
        Email email = new Email(
            "id",
            new Email.Address("foo@bar.com", "Mr. Foo Man"),
            createAddressList("foo@bar.com", "baz@potato.com"),
            randomFrom(Email.Priority.values()),
            ZonedDateTime.now(),
            createAddressList("foo@bar.com", "non-whitelisted@invalid.com"),
            null,
            null,
            "subject",
            "body",
            "htmlbody",
            Collections.emptyMap()
        );
        when(account.name()).thenReturn("account1");
        Authentication auth = new Authentication("user", new Secret("passwd".toCharArray()));
        Profile profile = randomFrom(Profile.values());

        // This send will fail because one of the recipients ("non-whitelisted@invalid.com") is in a domain that is not in the allowed list
        IllegalArgumentException e1 = expectThrows(
            IllegalArgumentException.class,
            () -> emailService.send(email, auth, profile, "account1")
        );
        assertThat(
            e1.getMessage(),
            containsString(
                "failed to send email with subject [subject] and recipients [non-whitelisted@invalid.com, foo@bar.com], "
                    + "one or more recipients is not specified in the domain allow list setting "
                    + "[xpack.notification.email.recipient_allowlist]."
            )
        );

        // Now dynamically add "invalid.com" to the list of allowed domains:
        Settings newSettings = Settings.builder()
            .putList("xpack.notification.email.recipient_allowlist", "*@bar.com", "*@invalid.com")
            .build();
        clusterSettings.applySettings(newSettings);
        // Still expect an exception because we're not actually sending the email, but it's no longer because the domain isn't allowed:
        IllegalArgumentException e2 = expectThrows(
            IllegalArgumentException.class,
            () -> emailService.send(email, auth, profile, "account1")
        );
        assertThat(e2.getMessage(), containsString("port out of range"));
    }

    private Email createTestEmail(String... recipients) throws UnsupportedEncodingException {
        return new Email(
            "id",
            new Email.Address("sender@domain.com", "Sender"),
            createAddressList(recipients),
            randomFrom(Email.Priority.values()),
            ZonedDateTime.now(),
            createAddressList(recipients),
            null,
            null,
            "subject",
            "body",
            "htmlbody",
            Collections.emptyMap()
        );
    }

    private static Email.AddressList createAddressList(String... emails) throws UnsupportedEncodingException {
        List<Email.Address> addresses = new ArrayList<>();
        for (String email : emails) {
            addresses.add(new Email.Address(email, randomAlphaOfLength(10)));
        }
        return new Email.AddressList(addresses);
    }
}
