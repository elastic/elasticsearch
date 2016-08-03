/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.notification.pagerduty;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.common.http.HttpClient;

/**
 * A component to store pagerduty credentials.
 */
public class PagerDutyService extends AbstractComponent {

    public static final Setting<Settings> PAGERDUTY_ACCOUNT_SETTING =
        Setting.groupSetting("xpack.notification.pagerduty.", Setting.Property.Dynamic, Setting.Property.NodeScope);

    private final HttpClient httpClient;
    private volatile PagerDutyAccounts accounts;

    public PagerDutyService(Settings settings, HttpClient httpClient, ClusterSettings clusterSettings) {
        super(settings);
        this.httpClient = httpClient;
        clusterSettings.addSettingsUpdateConsumer(PAGERDUTY_ACCOUNT_SETTING, this::setPagerDutyAccountSetting);
        setPagerDutyAccountSetting(PAGERDUTY_ACCOUNT_SETTING.get(settings));
    }

    private void setPagerDutyAccountSetting(Settings settings) {
        accounts = new PagerDutyAccounts(settings, httpClient, logger);
    }

    public PagerDutyAccount getDefaultAccount() {
        return accounts.account(null);
    }

    public PagerDutyAccount getAccount(String name) {
        return accounts.account(name);
    }
}
