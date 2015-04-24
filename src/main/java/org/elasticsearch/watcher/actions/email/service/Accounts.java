/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.email.service;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.watcher.support.secret.SecretService;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class Accounts {

    private final String defaultAccountName;
    private final Map<String, Account> accounts;

    public Accounts(Settings settings, SecretService secretService, ESLogger logger) {
        Settings accountsSettings = settings.getAsSettings("account");
        accounts = new HashMap<>();
        for (String name : accountsSettings.names()) {
            Account.Config config = new Account.Config(name, accountsSettings.getAsSettings(name));
            Account account = new Account(config, secretService, logger);
            accounts.put(name, account);
        }

        String defaultAccountName = settings.get("default_account");
        if (defaultAccountName == null) {
            if (accounts.isEmpty()) {
                this.defaultAccountName = null;
            } else {
                Account account = accounts.values().iterator().next();
                logger.info("default account set to [{}]", account.name());
                this.defaultAccountName = account.name();
            }
        } else if (!accounts.containsKey(defaultAccountName)) {
            throw new EmailSettingsException("could not fine default account [" + defaultAccountName + "]");
        } else {
            this.defaultAccountName = defaultAccountName;
        }
    }

    /**
     * Returns the account associated with the given name. If there is not such account, {@code null} is returned.
     * If the given name is {@code null}, the default account will be returned.
     *
     * @param name  The name of the requested account
     * @return      The account associated with the given name.
     */
    public Account account(String name) {
        if (name == null) {
            name = defaultAccountName;
        }
        return accounts.get(name);
    }

}
