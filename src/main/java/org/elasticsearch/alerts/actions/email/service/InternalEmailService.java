/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions.email.service;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

import javax.mail.MessagingException;

/**
 *
 */
public class InternalEmailService extends AbstractLifecycleComponent<InternalEmailService> implements EmailService {

    private volatile Accounts accounts;

    @Inject
    public InternalEmailService(Settings settings, NodeSettingsService nodeSettingsService) {
        super(settings);
        nodeSettingsService.addListener(new NodeSettingsService.Listener() {
            @Override
            public void onRefreshSettings(Settings settings) {
                reset(settings);
            }
        });
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
    public EmailSent send(Email email, Authentication auth, Profile profile) {
        return send(email, auth, profile, (String) null);
    }

    @Override
    public EmailSent send(Email email, Authentication auth, Profile profile, String accountName) {
        Account account = accounts.account(accountName);
        if (account == null) {
            throw new EmailException("failed to send email [" + email + "] via account [" + accountName + "]. account does not exist");
        }
        return send(email, auth, profile, account);
    }

    EmailSent send(Email email, Authentication auth, Profile profile, Account account) {
        assert account != null;
        try {
            account.send(email, auth, profile);
        } catch (MessagingException me) {
            throw new EmailException("failed to send email [" + email + "] via account [" + account.name() + "]", me);
        }
        return new EmailSent(account.name(), email);
    }

    void reset(Settings nodeSettings) {
        Settings settings = ImmutableSettings.builder()
                .put(componentSettings)
                .put(nodeSettings.getComponentSettings(InternalEmailService.class))
                .build();
        accounts = createAccounts(settings, logger);
    }

    protected Accounts createAccounts(Settings settings, ESLogger logger) {
        return new Accounts(settings, logger);
    }

}
