/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.notification.hipchat;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.common.http.HttpClient;

/**
 * A component to store hipchat credentials.
 */
public class HipChatService extends AbstractComponent {

    private final HttpClient httpClient;
    private volatile HipChatAccounts accounts;
    public static final Setting<Settings> HIPCHAT_ACCOUNT_SETTING =
        Setting.groupSetting("xpack.notification.hipchat.", Setting.Property.Dynamic, Setting.Property.NodeScope);

    public HipChatService(Settings settings, HttpClient httpClient, ClusterSettings clusterSettings) {
        super(settings);
        this.httpClient = httpClient;
        clusterSettings.addSettingsUpdateConsumer(HIPCHAT_ACCOUNT_SETTING, this::setHipchatAccountSetting);
        setHipchatAccountSetting(HIPCHAT_ACCOUNT_SETTING.get(settings));
    }

    private void setHipchatAccountSetting(Settings setting) {
        accounts = new HipChatAccounts(setting, httpClient, logger);
    }

    /**
     * @return The default hipchat account.
     */
    public HipChatAccount getDefaultAccount() {
        return accounts.account(null);
    }

    /**
     * @return  The account identified by the given name. If the given name is {@code null} the default
     *          account will be returned.
     */
    public HipChatAccount getAccount(String name) {
        return accounts.account(name);
    }

}
