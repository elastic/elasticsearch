/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.email.service;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.watcher.shield.WatcherSettingsFilter;
import org.elasticsearch.watcher.support.secret.SecretService;

import javax.mail.MessagingException;

/**
 *
 */
public class InternalEmailService extends AbstractLifecycleComponent<InternalEmailService> implements EmailService {

    private final SecretService secretService;

    private volatile Accounts accounts;

    @Inject
    public InternalEmailService(Settings settings, SecretService secretService, NodeSettingsService nodeSettingsService, WatcherSettingsFilter settingsFilter) {
        super(settings);
        this.secretService = secretService;
        nodeSettingsService.addListener(new NodeSettingsService.Listener() {
            @Override
            public void onRefreshSettings(Settings settings) {
                reset(settings);
            }
        });
        settingsFilter.filterOut("watcher.actions.email.service.account.*.smtp.password");
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        reset(settings);
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

    @Override
    public EmailSent send(Email email, Authentication auth, Profile profile) throws MessagingException {
        return send(email, auth, profile, (String) null);
    }

    @Override
    public EmailSent send(Email email, Authentication auth, Profile profile, String accountName) throws MessagingException {
        Account account = accounts.account(accountName);
        if (account == null) {
            throw new IllegalArgumentException("failed to send email with subject [" + email.subject() + "] via account [" + accountName + "]. account does not exist");
        }
        return send(email, auth, profile, account);
    }

    EmailSent send(Email email, Authentication auth, Profile profile, Account account) throws MessagingException {
        assert account != null;
        try {
            email = account.send(email, auth, profile);
        } catch (MessagingException me) {
            throw new MessagingException("failed to send email with subject [" + email.subject() + "] via account [" + account.name() + "]", me);
        }
        return new EmailSent(account.name(), email);
    }

    void reset(Settings nodeSettings) {
        Settings.Builder builder = Settings.builder();
        String prefix = "watcher.actions.email.service";
        for (String setting : settings.getAsMap().keySet()) {
            if (setting.startsWith(prefix)) {
                builder.put(setting.substring(prefix.length()+1), settings.get(setting));
            }
        }
        if (nodeSettings != settings) { // if it's the same settings, no point in re-applying it
            for (String setting : nodeSettings.getAsMap().keySet()) {
                if (setting.startsWith(prefix)) {
                    builder.put(setting.substring(prefix.length() + 1), nodeSettings.get(setting));
                }
            }
        }
        accounts = createAccounts(builder.build(), logger);
    }

    protected Accounts createAccounts(Settings settings, ESLogger logger) {
        return new Accounts(settings, secretService, logger);
    }

}
