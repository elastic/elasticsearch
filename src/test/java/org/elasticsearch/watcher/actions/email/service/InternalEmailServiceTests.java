/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.email.service;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.watcher.shield.WatcherSettingsFilter;
import org.elasticsearch.watcher.support.secret.Secret;
import org.elasticsearch.watcher.support.secret.SecretService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.*;

/**
 *
 */
public class InternalEmailServiceTests extends ElasticsearchTestCase {

    private InternalEmailService service;
    private Accounts accounts;

    @Before
    public void init() throws Exception {
        accounts = mock(Accounts.class);
        service = new InternalEmailService(ImmutableSettings.EMPTY, new SecretService.PlainText(), new NodeSettingsService(ImmutableSettings.EMPTY), WatcherSettingsFilter.Noop.INSTANCE) {
            @Override
            protected Accounts createAccounts(Settings settings, ESLogger logger) {
                return accounts;
            }
        };
        service.start();
    }

    @After
    public void cleanup() throws Exception {
        service.stop();
    }

    @Test
    public void testSend() throws Exception {
        Account account = mock(Account.class);
        when(account.name()).thenReturn("account1");
        when(accounts.account("account1")).thenReturn(account);
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

}
