/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.email;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.watcher.crypto.CryptoService;
import org.elasticsearch.xpack.watcher.notification.NotificationService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.mail.MessagingException;
import javax.mail.internet.InternetAddress;
import javax.net.ssl.SSLSocketFactory;

import static org.elasticsearch.xpack.core.watcher.WatcherField.EMAIL_NOTIFICATION_SSL_PREFIX;

/**
 * A component to store email credentials and handle sending email notifications.
 */
public class EmailService extends NotificationService<Account> {

    private static final Setting<String> SETTING_DEFAULT_ACCOUNT = Setting.simpleString(
        "xpack.notification.email.default_account",
        Property.Dynamic,
        Property.NodeScope
    );

    private static final Setting.AffixSetting<String> SETTING_PROFILE = Setting.affixKeySetting(
        "xpack.notification.email.account.",
        "profile",
        (key) -> Setting.simpleString(key, Property.Dynamic, Property.NodeScope)
    );

    private static final List<String> ALLOW_ALL_DEFAULT = List.of("*");

    private static final Setting<List<String>> SETTING_DOMAIN_ALLOWLIST = Setting.stringListSetting(
        "xpack.notification.email.account.domain_allowlist",
        ALLOW_ALL_DEFAULT,
        new Setting.Validator<>() {
            @Override
            public void validate(List<String> value) {
                // Ignored
            }

            @Override
            @SuppressWarnings("unchecked")
            public void validate(List<String> value, Map<Setting<?>, Object> settings) {
                List<String> recipientAllowPatterns = (List<String>) settings.get(SETTING_RECIPIENT_ALLOW_PATTERNS);
                if (value.equals(ALLOW_ALL_DEFAULT) == false && recipientAllowPatterns.equals(ALLOW_ALL_DEFAULT) == false) {
                    throw new IllegalArgumentException(
                        "Cannot set both ["
                            + SETTING_RECIPIENT_ALLOW_PATTERNS.getKey()
                            + "] and ["
                            + SETTING_DOMAIN_ALLOWLIST.getKey()
                            + "] to a non [\"*\"] value at the same time."
                    );
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                List<Setting<?>> settingRecipientAllowPatterns = List.of(SETTING_RECIPIENT_ALLOW_PATTERNS);
                return settingRecipientAllowPatterns.iterator();
            }
        },
        Property.Dynamic,
        Property.NodeScope
    );

    private static final Setting<List<String>> SETTING_RECIPIENT_ALLOW_PATTERNS = Setting.stringListSetting(
        "xpack.notification.email.recipient_allowlist",
        ALLOW_ALL_DEFAULT,
        new Setting.Validator<>() {
            @Override
            public void validate(List<String> value) {
                // Ignored
            }

            @Override
            @SuppressWarnings("unchecked")
            public void validate(List<String> value, Map<Setting<?>, Object> settings) {
                List<String> domainAllowList = (List<String>) settings.get(SETTING_DOMAIN_ALLOWLIST);
                if (value.equals(ALLOW_ALL_DEFAULT) == false && domainAllowList.equals(ALLOW_ALL_DEFAULT) == false) {
                    throw new IllegalArgumentException(
                        "Connect set both ["
                            + SETTING_RECIPIENT_ALLOW_PATTERNS.getKey()
                            + "] and ["
                            + SETTING_DOMAIN_ALLOWLIST.getKey()
                            + "] to a non [\"*\"] value at the same time."
                    );
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                List<Setting<?>> settingDomainAllowlist = List.of(SETTING_DOMAIN_ALLOWLIST);
                return settingDomainAllowlist.iterator();
            }
        },
        Property.Dynamic,
        Property.NodeScope
    );

    private static final Setting.AffixSetting<Settings> SETTING_EMAIL_DEFAULTS = Setting.affixKeySetting(
        "xpack.notification.email.account.",
        "email_defaults",
        (key) -> Setting.groupSetting(key + ".", Property.Dynamic, Property.NodeScope)
    );

    // settings that can be configured as smtp properties
    private static final Setting.AffixSetting<Boolean> SETTING_SMTP_AUTH = Setting.affixKeySetting(
        "xpack.notification.email.account.",
        "smtp.auth",
        (key) -> Setting.boolSetting(key, false, Property.Dynamic, Property.NodeScope)
    );

    private static final Setting.AffixSetting<Boolean> SETTING_SMTP_STARTTLS_ENABLE = Setting.affixKeySetting(
        "xpack.notification.email.account.",
        "smtp.starttls.enable",
        (key) -> Setting.boolSetting(key, false, Property.Dynamic, Property.NodeScope)
    );

    private static final Setting.AffixSetting<Boolean> SETTING_SMTP_STARTTLS_REQUIRED = Setting.affixKeySetting(
        "xpack.notification.email.account.",
        "smtp.starttls.required",
        (key) -> Setting.boolSetting(key, false, Property.Dynamic, Property.NodeScope)
    );

    private static final Setting.AffixSetting<String> SETTING_SMTP_HOST = Setting.affixKeySetting(
        "xpack.notification.email.account.",
        "smtp.host",
        (key) -> Setting.simpleString(key, Property.Dynamic, Property.NodeScope)
    );

    private static final Setting.AffixSetting<Integer> SETTING_SMTP_PORT = Setting.affixKeySetting(
        "xpack.notification.email.account.",
        "smtp.port",
        (key) -> Setting.intSetting(key, 587, Property.Dynamic, Property.NodeScope)
    );

    private static final Setting.AffixSetting<String> SETTING_SMTP_USER = Setting.affixKeySetting(
        "xpack.notification.email.account.",
        "smtp.user",
        (key) -> Setting.simpleString(key, Property.Dynamic, Property.NodeScope)
    );

    private static final Setting.AffixSetting<SecureString> SETTING_SECURE_PASSWORD = Setting.affixKeySetting(
        "xpack.notification.email.account.",
        "smtp.secure_password",
        (key) -> SecureSetting.secureString(key, null)
    );

    private static final Setting.AffixSetting<TimeValue> SETTING_SMTP_TIMEOUT = Setting.affixKeySetting(
        "xpack.notification.email.account.",
        "smtp.timeout",
        (key) -> Setting.timeSetting(key, TimeValue.timeValueMinutes(2), Property.Dynamic, Property.NodeScope)
    );

    private static final Setting.AffixSetting<TimeValue> SETTING_SMTP_CONNECTION_TIMEOUT = Setting.affixKeySetting(
        "xpack.notification.email.account.",
        "smtp.connection_timeout",
        (key) -> Setting.timeSetting(key, TimeValue.timeValueMinutes(2), Property.Dynamic, Property.NodeScope)
    );

    private static final Setting.AffixSetting<TimeValue> SETTING_SMTP_WRITE_TIMEOUT = Setting.affixKeySetting(
        "xpack.notification.email.account.",
        "smtp.write_timeout",
        (key) -> Setting.timeSetting(key, TimeValue.timeValueMinutes(2), Property.Dynamic, Property.NodeScope)
    );

    private static final Setting.AffixSetting<String> SETTING_SMTP_LOCAL_ADDRESS = Setting.affixKeySetting(
        "xpack.notification.email.account.",
        "smtp.local_address",
        (key) -> Setting.simpleString(key, Property.Dynamic, Property.NodeScope)
    );

    private static final Setting.AffixSetting<String> SETTING_SMTP_SSL_TRUST_ADDRESS = Setting.affixKeySetting(
        "xpack.notification.email.account.",
        "smtp.ssl.trust",
        (key) -> Setting.simpleString(key, Property.Dynamic, Property.NodeScope)
    );

    private static final Setting.AffixSetting<Integer> SETTING_SMTP_LOCAL_PORT = Setting.affixKeySetting(
        "xpack.notification.email.account.",
        "smtp.local_port",
        (key) -> Setting.intSetting(key, 25, Property.Dynamic, Property.NodeScope)
    );

    private static final Setting.AffixSetting<Boolean> SETTING_SMTP_SEND_PARTIAL = Setting.affixKeySetting(
        "xpack.notification.email.account.",
        "smtp.send_partial",
        (key) -> Setting.boolSetting(key, false, Property.Dynamic, Property.NodeScope)
    );

    private static final Setting.AffixSetting<Boolean> SETTING_SMTP_WAIT_ON_QUIT = Setting.affixKeySetting(
        "xpack.notification.email.account.",
        "smtp.wait_on_quit",
        (key) -> Setting.boolSetting(key, true, Property.Dynamic, Property.NodeScope)
    );

    private static final SSLConfigurationSettings SSL_SETTINGS = SSLConfigurationSettings.withPrefix(EMAIL_NOTIFICATION_SSL_PREFIX, true);

    private static final Logger logger = LogManager.getLogger(EmailService.class);

    private final CryptoService cryptoService;
    private final SSLService sslService;
    private volatile Set<String> allowedDomains;
    private volatile Set<String> allowedRecipientPatterns;

    @SuppressWarnings("this-escape")
    public EmailService(Settings settings, @Nullable CryptoService cryptoService, SSLService sslService, ClusterSettings clusterSettings) {
        super("email", settings, clusterSettings, EmailService.getDynamicSettings(), EmailService.getSecureSettings());
        this.cryptoService = cryptoService;
        this.sslService = sslService;
        // ensure logging of setting changes
        clusterSettings.addSettingsUpdateConsumer(SETTING_DEFAULT_ACCOUNT, (s) -> {});
        clusterSettings.addAffixUpdateConsumer(SETTING_PROFILE, (s, o) -> {}, (s, o) -> {});
        clusterSettings.addAffixUpdateConsumer(SETTING_EMAIL_DEFAULTS, (s, o) -> {}, (s, o) -> {});
        clusterSettings.addAffixUpdateConsumer(SETTING_SMTP_AUTH, (s, o) -> {}, (s, o) -> {});
        clusterSettings.addAffixUpdateConsumer(SETTING_SMTP_STARTTLS_ENABLE, (s, o) -> {}, (s, o) -> {});
        clusterSettings.addAffixUpdateConsumer(SETTING_SMTP_STARTTLS_REQUIRED, (s, o) -> {}, (s, o) -> {});
        clusterSettings.addAffixUpdateConsumer(SETTING_SMTP_HOST, (s, o) -> {}, (s, o) -> {});
        clusterSettings.addAffixUpdateConsumer(SETTING_SMTP_PORT, (s, o) -> {}, (s, o) -> {});
        clusterSettings.addAffixUpdateConsumer(SETTING_SMTP_USER, (s, o) -> {}, (s, o) -> {});
        clusterSettings.addAffixUpdateConsumer(SETTING_SMTP_TIMEOUT, (s, o) -> {}, (s, o) -> {});
        clusterSettings.addAffixUpdateConsumer(SETTING_SMTP_CONNECTION_TIMEOUT, (s, o) -> {}, (s, o) -> {});
        clusterSettings.addAffixUpdateConsumer(SETTING_SMTP_WRITE_TIMEOUT, (s, o) -> {}, (s, o) -> {});
        clusterSettings.addAffixUpdateConsumer(SETTING_SMTP_SSL_TRUST_ADDRESS, (s, o) -> {}, (s, o) -> {});
        clusterSettings.addAffixUpdateConsumer(SETTING_SMTP_LOCAL_ADDRESS, (s, o) -> {}, (s, o) -> {});
        clusterSettings.addAffixUpdateConsumer(SETTING_SMTP_LOCAL_PORT, (s, o) -> {}, (s, o) -> {});
        clusterSettings.addAffixUpdateConsumer(SETTING_SMTP_SEND_PARTIAL, (s, o) -> {}, (s, o) -> {});
        clusterSettings.addAffixUpdateConsumer(SETTING_SMTP_WAIT_ON_QUIT, (s, o) -> {}, (s, o) -> {});
        this.allowedDomains = new HashSet<>(SETTING_DOMAIN_ALLOWLIST.get(settings));
        this.allowedRecipientPatterns = new HashSet<>(SETTING_RECIPIENT_ALLOW_PATTERNS.get(settings));
        clusterSettings.addSettingsUpdateConsumer(SETTING_DOMAIN_ALLOWLIST, this::updateAllowedDomains);
        clusterSettings.addSettingsUpdateConsumer(SETTING_RECIPIENT_ALLOW_PATTERNS, this::updateAllowedRecipientPatterns);
        // do an initial load
        reload(settings);
    }

    void updateAllowedDomains(List<String> newDomains) {
        this.allowedDomains = new HashSet<>(newDomains);
    }

    void updateAllowedRecipientPatterns(List<String> newPatterns) {
        this.allowedRecipientPatterns = new HashSet<>(newPatterns);
    }

    @Override
    protected Account createAccount(String name, Settings accountSettings) {
        Account.Config config = new Account.Config(name, accountSettings, getSmtpSslSocketFactory(), logger);
        return new Account(config, cryptoService, logger);
    }

    @Nullable
    private SSLSocketFactory getSmtpSslSocketFactory() {
        final SslConfiguration sslConfiguration = sslService.getSSLConfiguration(EMAIL_NOTIFICATION_SSL_PREFIX);
        if (sslConfiguration == null || sslConfiguration.explicitlyConfigured() == false) {
            return null;
        }
        return sslService.sslSocketFactory(sslConfiguration);
    }

    public EmailSent send(Email email, Authentication auth, Profile profile, String accountName) throws MessagingException {
        Account account = getAccount(accountName);
        if (account == null) {
            throw new IllegalArgumentException(
                "failed to send email with subject [" + email.subject() + "] via account [" + accountName + "]. account does not exist"
            );
        }
        if (recipientDomainsInAllowList(email, this.allowedDomains) == false) {
            throw new IllegalArgumentException(
                "failed to send email with subject ["
                    + email.subject()
                    + "] and recipient domains "
                    + getRecipients(email, true)
                    + ", one or more recipients is not specified in the domain allow list setting ["
                    + SETTING_DOMAIN_ALLOWLIST.getKey()
                    + "]."
            );
        }
        if (recipientAddressInAllowList(email, this.allowedRecipientPatterns) == false) {
            throw new IllegalArgumentException(
                "failed to send email with subject ["
                    + email.subject()
                    + "] and recipients "
                    + getRecipients(email, false)
                    + ", one or more recipients is not specified in the domain allow list setting ["
                    + SETTING_RECIPIENT_ALLOW_PATTERNS.getKey()
                    + "]."
            );
        }
        return send(email, auth, profile, account);
    }

    // Visible for testing
    static Set<String> getRecipients(Email email, boolean domainsOnly) {
        var stream = Stream.concat(
            Optional.ofNullable(email.to()).map(addrs -> Arrays.stream(addrs.toArray())).orElse(Stream.empty()),
            Stream.concat(
                Optional.ofNullable(email.cc()).map(addrs -> Arrays.stream(addrs.toArray())).orElse(Stream.empty()),
                Optional.ofNullable(email.bcc()).map(addrs -> Arrays.stream(addrs.toArray())).orElse(Stream.empty())
            )
        ).map(InternetAddress::getAddress);

        if (domainsOnly) {
            // Pull out only the domain of the email address, so foo@bar.com becomes bar.com
            stream = stream.map(emailAddress -> emailAddress.substring(emailAddress.lastIndexOf('@') + 1));
        }

        return stream.collect(Collectors.toSet());
    }

    // Visible for testing
    static boolean recipientDomainsInAllowList(Email email, Set<String> allowedDomainSet) {
        if (allowedDomainSet.isEmpty()) {
            // Nothing is allowed
            return false;
        }
        if (allowedDomainSet.contains("*")) {
            // Don't bother checking, because there is a wildcard all
            return true;
        }
        final Set<String> domains = getRecipients(email, true);
        final Predicate<String> matchesAnyAllowedDomain = domain -> allowedDomainSet.stream()
            .anyMatch(allowedDomain -> Regex.simpleMatch(allowedDomain, domain, true));
        return domains.stream().allMatch(matchesAnyAllowedDomain);
    }

    // Visible for testing
    static boolean recipientAddressInAllowList(Email email, Set<String> allowedRecipientPatterns) {
        if (allowedRecipientPatterns.isEmpty()) {
            // Nothing is allowed
            return false;
        }
        if (allowedRecipientPatterns.contains("*")) {
            // Don't bother checking, because there is a wildcard all
            return true;
        }

        final Set<String> recipients = getRecipients(email, false);
        final Predicate<String> matchesAnyAllowedRecipient = recipient -> allowedRecipientPatterns.stream()
            .anyMatch(pattern -> Regex.simpleMatch(pattern, recipient, true));
        return recipients.stream().allMatch(matchesAnyAllowedRecipient);
    }

    private static EmailSent send(Email email, Authentication auth, Profile profile, Account account) throws MessagingException {
        assert account != null;
        try {
            email = account.send(email, auth, profile);
        } catch (MessagingException me) {
            throw new MessagingException(
                "failed to send email with subject [" + email.subject() + "] via account [" + account.name() + "]",
                me
            );
        }
        return new EmailSent(account.name(), email);
    }

    public static class EmailSent {

        private final String account;
        private final Email email;

        public EmailSent(String account, Email email) {
            this.account = account;
            this.email = email;
        }

        public String account() {
            return account;
        }

        public Email email() {
            return email;
        }
    }

    private static List<Setting<?>> getDynamicSettings() {
        return Arrays.asList(
            SETTING_DEFAULT_ACCOUNT,
            SETTING_DOMAIN_ALLOWLIST,
            SETTING_RECIPIENT_ALLOW_PATTERNS,
            SETTING_PROFILE,
            SETTING_EMAIL_DEFAULTS,
            SETTING_SMTP_AUTH,
            SETTING_SMTP_HOST,
            SETTING_SMTP_PORT,
            SETTING_SMTP_STARTTLS_ENABLE,
            SETTING_SMTP_USER,
            SETTING_SMTP_STARTTLS_REQUIRED,
            SETTING_SMTP_TIMEOUT,
            SETTING_SMTP_CONNECTION_TIMEOUT,
            SETTING_SMTP_WRITE_TIMEOUT,
            SETTING_SMTP_LOCAL_ADDRESS,
            SETTING_SMTP_LOCAL_PORT,
            SETTING_SMTP_SEND_PARTIAL,
            SETTING_SMTP_WAIT_ON_QUIT,
            SETTING_SMTP_SSL_TRUST_ADDRESS
        );
    }

    private static List<Setting<?>> getSecureSettings() {
        return Arrays.asList(SETTING_SECURE_PASSWORD);
    }

    public static List<Setting<?>> getSettings() {
        List<Setting<?>> allSettings = new ArrayList<Setting<?>>(EmailService.getDynamicSettings());
        allSettings.addAll(EmailService.getSecureSettings());
        allSettings.addAll(SSL_SETTINGS.getEnabledSettings());
        return allSettings;
    }

}
