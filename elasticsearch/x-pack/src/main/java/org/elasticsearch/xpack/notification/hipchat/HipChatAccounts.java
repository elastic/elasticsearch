/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.notification.hipchat;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.xpack.notification.hipchat.HipChatAccount.Profile;
import org.elasticsearch.watcher.support.http.HttpClient;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class HipChatAccounts {

    private final Map<String, HipChatAccount> accounts;
    private final String defaultAccountName;

    public HipChatAccounts(Settings settings, HttpClient httpClient, ESLogger logger) {
        HipChatServer defaultServer = new HipChatServer(settings);
        Settings accountsSettings = settings.getAsSettings("account");
        accounts = new HashMap<>();
        for (String name : accountsSettings.names()) {
            Settings accountSettings = accountsSettings.getAsSettings(name);
            Profile profile = Profile.resolve(accountSettings, "profile", null);
            if (profile == null) {
                throw new SettingsException("missing [profile] setting for hipchat account [" + name + "]");
            }
            HipChatAccount account = profile.createAccount(name, accountSettings, defaultServer, httpClient, logger);
            accounts.put(name, account);
        }

        String defaultAccountName = settings.get("default_account");
        if (defaultAccountName == null) {
            if (accounts.isEmpty()) {
                this.defaultAccountName = null;
            } else {
                HipChatAccount account = accounts.values().iterator().next();
                logger.info("default hipchat account set to [{}]", account.name);
                this.defaultAccountName = account.name;
            }
        } else if (!accounts.containsKey(defaultAccountName)) {
            throw new SettingsException("could not find default hipchat account [" + defaultAccountName + "]");
        } else {
            this.defaultAccountName = defaultAccountName;
        }
    }

    /**
     * Returns the account associated with the given name. If there is not such account, {@code null} is returned.
     * If the given name is {@code null}, the default account will be returned.
     *
     * @param name  The name of the requested account
     * @return      The account associated with the given name, or {@code null} when requested an unkonwn account.
     * @throws      IllegalStateException if the name is null and the default account is null.
     */
    public HipChatAccount account(String name) throws IllegalStateException {
        if (name == null) {
            if (defaultAccountName == null) {
                throw new IllegalStateException("cannot find default hipchat account as no accounts have been configured");
            }
            name = defaultAccountName;
        }
        return accounts.get(name);
    }
}
