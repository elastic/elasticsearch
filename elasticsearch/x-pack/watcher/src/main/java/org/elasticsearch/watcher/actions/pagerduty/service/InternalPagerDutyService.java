/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.pagerduty.service;

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.watcher.shield.WatcherSettingsFilter;
import org.elasticsearch.watcher.support.http.HttpClient;

/**
 *
 */
public class InternalPagerDutyService extends AbstractLifecycleComponent<PagerDutyService> implements PagerDutyService {

    public static final Setting<Settings> PAGERDUTY_ACCOUNT_SETTING = Setting.groupSetting("watcher.actions.pagerduty.service.", true, Setting.Scope.CLUSTER);

    private final HttpClient httpClient;
    private volatile PagerDutyAccounts accounts;

    @Inject
    public InternalPagerDutyService(Settings settings, HttpClient httpClient, ClusterSettings clusterSettings,
                                    WatcherSettingsFilter settingsFilter) {
        super(settings);
        this.httpClient = httpClient;
        settingsFilter.filterOut(
                "watcher.actions.pagerduty.service." + PagerDutyAccount.SERVICE_KEY_SETTING,
                "watcher.actions.pagerduty.service.account.*." + PagerDutyAccount.SERVICE_KEY_SETTING
        );
        clusterSettings.addSettingsUpdateConsumer(PAGERDUTY_ACCOUNT_SETTING, this::setPagerDutyAccountSetting);
    }

    @Override
    protected void doStart() {
        setPagerDutyAccountSetting(PAGERDUTY_ACCOUNT_SETTING.get(settings));
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() {
    }

    private void setPagerDutyAccountSetting(Settings settings) {
        accounts = new PagerDutyAccounts(settings, httpClient, logger);
    }

    @Override
    public PagerDutyAccount getDefaultAccount() {
        return accounts.account(null);
    }

    @Override
    public PagerDutyAccount getAccount(String name) {
        return accounts.account(name);
    }
}
