/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification.email;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.watcher.crypto.CryptoService;
import org.elasticsearch.xpack.watcher.notification.NotificationService;

import javax.mail.MessagingException;

/**
 * A component to store email credentials and handle sending email notifications.
 */
public class EmailService extends NotificationService<Account> {

    private final CryptoService cryptoService;
    public static final Setting<Settings> EMAIL_ACCOUNT_SETTING =
        Setting.groupSetting("xpack.notification.email.", Setting.Property.Dynamic, Setting.Property.NodeScope);

    public EmailService(Settings settings, @Nullable CryptoService cryptoService, ClusterSettings clusterSettings) {
        super(settings, "email");
        this.cryptoService = cryptoService;
        clusterSettings.addSettingsUpdateConsumer(EMAIL_ACCOUNT_SETTING, this::setAccountSetting);
        setAccountSetting(EMAIL_ACCOUNT_SETTING.get(settings));
    }

    @Override
    protected Account createAccount(String name, Settings accountSettings) {
        Account.Config config = new Account.Config(name, accountSettings);
        return new Account(config, cryptoService, logger);
    }

    public EmailSent send(Email email, Authentication auth, Profile profile, String accountName) throws MessagingException {
        Account account = getAccount(accountName);
        if (account == null) {
            throw new IllegalArgumentException("failed to send email with subject [" + email.subject() + "] via account [" + accountName
                + "]. account does not exist");
        }
        return send(email, auth, profile, account);
    }

    private EmailSent send(Email email, Authentication auth, Profile profile, Account account) throws MessagingException {
        assert account != null;
        try {
            email = account.send(email, auth, profile);
        } catch (MessagingException me) {
            throw new MessagingException("failed to send email with subject [" + email.subject() + "] via account [" + account.name() +
                "]", me);
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

}
