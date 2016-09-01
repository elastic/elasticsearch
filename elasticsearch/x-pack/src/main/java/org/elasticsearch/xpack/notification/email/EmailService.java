/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.notification.email;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.security.crypto.CryptoService;

import javax.mail.MessagingException;

/**
 * A component to store email credentials and handle sending email notifications.
 */
public class EmailService extends AbstractComponent {

    private final CryptoService cryptoService;
    public static final Setting<Settings> EMAIL_ACCOUNT_SETTING =
        Setting.groupSetting("xpack.notification.email.", Setting.Property.Dynamic, Setting.Property.NodeScope);

    private volatile Accounts accounts;

    public EmailService(Settings settings, @Nullable CryptoService cryptoService, ClusterSettings clusterSettings) {
        super(settings);
        this.cryptoService = cryptoService;
        clusterSettings.addSettingsUpdateConsumer(EMAIL_ACCOUNT_SETTING, this::setEmailAccountSettings);
        setEmailAccountSettings(EMAIL_ACCOUNT_SETTING.get(settings));
    }

    private void setEmailAccountSettings(Settings settings) {
        this.accounts = createAccounts(settings, logger);
    }

    public EmailSent send(Email email, Authentication auth, Profile profile) throws MessagingException {
        return send(email, auth, profile, (String) null);
    }

    public EmailSent send(Email email, Authentication auth, Profile profile, String accountName) throws MessagingException {
        Account account = accounts.account(accountName);
        if (account == null) {
            throw new IllegalArgumentException("failed to send email with subject [" + email.subject() + "] via account [" + accountName
                + "]. account does not exist");
        }
        return send(email, auth, profile, account);
    }

    EmailSent send(Email email, Authentication auth, Profile profile, Account account) throws MessagingException {
        assert account != null;
        try {
            email = account.send(email, auth, profile);
        } catch (MessagingException me) {
            throw new MessagingException("failed to send email with subject [" + email.subject() + "] via account [" + account.name() +
                "]", me);
        }
        return new EmailSent(account.name(), email);
    }

    protected Accounts createAccounts(Settings settings, Logger logger) {
        return new Accounts(settings, cryptoService, logger);
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
