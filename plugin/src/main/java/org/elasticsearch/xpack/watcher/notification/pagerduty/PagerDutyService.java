/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification.pagerduty;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.elasticsearch.xpack.watcher.notification.NotificationService;

/**
 * A component to store pagerduty credentials.
 */
public class PagerDutyService extends NotificationService<PagerDutyAccount> {

    public static final Setting<Settings> PAGERDUTY_ACCOUNT_SETTING =
        Setting.groupSetting("xpack.notification.pagerduty.", Setting.Property.Dynamic, Setting.Property.NodeScope);

    private final HttpClient httpClient;

    public PagerDutyService(Settings settings, HttpClient httpClient, ClusterSettings clusterSettings) {
        super(settings, "pagerduty");
        this.httpClient = httpClient;
        clusterSettings.addSettingsUpdateConsumer(PAGERDUTY_ACCOUNT_SETTING, this::setAccountSetting);
        setAccountSetting(PAGERDUTY_ACCOUNT_SETTING.get(settings));
    }

    @Override
    protected PagerDutyAccount createAccount(String name, Settings accountSettings) {
        return new PagerDutyAccount(name, accountSettings, accountSettings, httpClient, logger);
    }
}
