/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.notification.slack;

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.watcher.support.http.HttpClient;

/**
 *
 */
public class InternalSlackService extends AbstractLifecycleComponent<SlackService> implements SlackService {

    private final HttpClient httpClient;
    public static final Setting<Settings> SLACK_ACCOUNT_SETTING =
            Setting.groupSetting("xpack.notification.slack.", Setting.Property.Dynamic, Setting.Property.NodeScope);
    private volatile SlackAccounts accounts;

    @Inject
    public InternalSlackService(Settings settings, HttpClient httpClient, ClusterSettings clusterSettings) {
        super(settings);
        this.httpClient = httpClient;
        clusterSettings.addSettingsUpdateConsumer(SLACK_ACCOUNT_SETTING, this::setSlackAccountSetting);
    }

    @Override
    protected void doStart() {
        setSlackAccountSetting(SLACK_ACCOUNT_SETTING.get(settings));
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() {
    }

    @Override
    public SlackAccount getDefaultAccount() {
        return accounts.account(null);
    }

    private void setSlackAccountSetting(Settings setting) {
        accounts = new SlackAccounts(setting, httpClient, logger);
    }

    @Override
    public SlackAccount getAccount(String name) {
        return accounts.account(name);
    }
}
