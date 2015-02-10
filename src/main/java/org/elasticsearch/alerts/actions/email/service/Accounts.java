/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions.email.service;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class Accounts {

    private final String defaultAccountName;
    private final Map<String, Account> accounts;

    public Accounts(Settings settings, ESLogger logger) {
        settings = settings.getAsSettings("account");
        Map<String, Account> accounts = new HashMap<>();
        for (String name : settings.names()) {
            Account.Config config = new Account.Config(name, settings.getAsSettings(name));
            Account account = new Account(config, logger);
            accounts.put(name, account);
        }

        if (accounts.isEmpty()) {
            this.accounts = Collections.emptyMap();
            this.defaultAccountName = null;
        } else {
            this.accounts = accounts;
            String defaultAccountName = settings.get("default_account");
            if (defaultAccountName == null) {
                Account account = accounts.values().iterator().next();
                logger.error("default account set to [{}]", account.name());
                this.defaultAccountName = account.name();
            } else if (!accounts.containsKey(defaultAccountName)) {
                Account account = accounts.values().iterator().next();
                this.defaultAccountName = account.name();
                logger.error("could not find configured default account [{}]. falling back on account [{}]", defaultAccountName, account.name());
            } else {
                this.defaultAccountName = defaultAccountName;
            }
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
