/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification.hipchat;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.elasticsearch.xpack.watcher.notification.NotificationService;

/**
 * A component to store hipchat credentials.
 */
public class HipChatService extends NotificationService<HipChatAccount> {

    private final HttpClient httpClient;
    public static final Setting<Settings> HIPCHAT_ACCOUNT_SETTING =
        Setting.groupSetting("xpack.notification.hipchat.", Setting.Property.Dynamic, Setting.Property.NodeScope);
    private HipChatServer defaultServer;

    public HipChatService(Settings settings, HttpClient httpClient, ClusterSettings clusterSettings) {
        super(settings, "hipchat");
        this.httpClient = httpClient;
        clusterSettings.addSettingsUpdateConsumer(HIPCHAT_ACCOUNT_SETTING, this::setAccountSetting);
        setAccountSetting(HIPCHAT_ACCOUNT_SETTING.get(settings));
    }

    @Override
    protected synchronized void setAccountSetting(Settings settings) {
        defaultServer = new HipChatServer(settings);
        super.setAccountSetting(settings);
    }

    @Override
    protected HipChatAccount createAccount(String name, Settings accountSettings) {
        HipChatAccount.Profile profile = HipChatAccount.Profile.resolve(accountSettings, "profile", null);
        if (profile == null) {
            throw new SettingsException("missing [profile] setting for hipchat account [" + name + "]");
        }
        return profile.createAccount(name, accountSettings, defaultServer, httpClient, logger);
    }
}
