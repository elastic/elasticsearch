/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.notification.pagerduty;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.watcher.support.http.HttpClient;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class PagerDutyAccounts {

    private final Map<String, PagerDutyAccount> accounts;
    private final String defaultAccountName;

    public PagerDutyAccounts(Settings serviceSettings, HttpClient httpClient, ESLogger logger) {
        Settings accountsSettings = serviceSettings.getAsSettings("account");
        accounts = new HashMap<>();
        for (String name : accountsSettings.names()) {
            Settings accountSettings = accountsSettings.getAsSettings(name);
            PagerDutyAccount account = new PagerDutyAccount(name, accountSettings, serviceSettings, httpClient, logger);
            accounts.put(name, account);
        }

        String defaultAccountName = serviceSettings.get("default_account");
        if (defaultAccountName == null) {
            if (accounts.isEmpty()) {
                this.defaultAccountName = null;
            } else {
                PagerDutyAccount account = accounts.values().iterator().next();
                logger.info("default pager duty account set to [{}]", account.name);
                this.defaultAccountName = account.name;
            }
        } else if (!accounts.containsKey(defaultAccountName)) {
            throw new SettingsException("could not find default pagerduty account [" + defaultAccountName + "]");
        } else {
            this.defaultAccountName = defaultAccountName;
        }
    }

    /**
     * Returns the account associated with the given name. If there is not such account, {@code null} is returned.
     * If the given name is {@code null}, the default account will be returned.
     *
     * @param name  The name of the requested account
     * @return      The account associated with the given name, or {@code null} when requested an unknown account.
     * @throws      IllegalStateException if the name is null and the default account is null.
     */
    public PagerDutyAccount account(String name) throws IllegalStateException {
        if (name == null) {
            if (defaultAccountName == null) {
                throw new IllegalStateException("cannot find default pagerduty account as no accounts have been configured");
            }
            name = defaultAccountName;
        }
        return accounts.get(name);
    }
}
