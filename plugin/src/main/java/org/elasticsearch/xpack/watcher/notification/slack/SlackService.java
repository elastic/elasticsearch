/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification.slack;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.elasticsearch.xpack.watcher.notification.NotificationService;

/**
 * A component to store slack credentials.
 */
public class SlackService extends NotificationService<SlackAccount> {

    public static final Setting<Settings> SLACK_ACCOUNT_SETTING =
        Setting.groupSetting("xpack.notification.slack.", Setting.Property.Dynamic, Setting.Property.NodeScope);

    private final HttpClient httpClient;

    public SlackService(Settings settings, HttpClient httpClient, ClusterSettings clusterSettings) {
        super(settings, "slack");
        this.httpClient = httpClient;
        clusterSettings.addSettingsUpdateConsumer(SLACK_ACCOUNT_SETTING, this::setAccountSetting);
        setAccountSetting(SLACK_ACCOUNT_SETTING.get(settings));
    }

    @Override
    protected SlackAccount createAccount(String name, Settings accountSettings) {
        return new SlackAccount(name, accountSettings, accountSettings, httpClient, logger);
    }
}
