/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.hipchat.service;

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.watcher.shield.WatcherSettingsFilter;
import org.elasticsearch.watcher.support.http.HttpClient;

/**
 *
 */
public class InternalHipChatService extends AbstractLifecycleComponent<InternalHipChatService> implements HipChatService {

    private final HttpClient httpClient;
    private volatile HipChatAccounts accounts;

    @Inject
    public InternalHipChatService(Settings settings, HttpClient httpClient, NodeSettingsService nodeSettingsService, WatcherSettingsFilter settingsFilter) {
        super(settings);
        this.httpClient = httpClient;
        nodeSettingsService.addListener(new NodeSettingsService.Listener() {
            @Override
            public void onRefreshSettings(Settings settings) {
                reset(settings);
            }
        });
        settingsFilter.filterOut("watcher.actions.hipchat.service.account.*.auth_token");
    }

    @Override
    protected void doStart() {
        reset(settings);
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() {
    }

    @Override
    public HipChatAccount getDefaultAccount() {
        return accounts.account(null);
    }

    @Override
    public HipChatAccount getAccount(String name) {
        return accounts.account(name);
    }

    void reset(Settings nodeSettings) {
        Settings.Builder builder = Settings.builder();
        String prefix = "watcher.actions.hipchat.service";
        for (String setting : settings.getAsMap().keySet()) {
            if (setting.startsWith(prefix)) {
                builder.put(setting.substring(prefix.length()+1), settings.get(setting));
            }
        }
        if (nodeSettings != settings) { // if it's the same settings, no point in re-applying it
            for (String setting : nodeSettings.getAsMap().keySet()) {
                if (setting.startsWith(prefix)) {
                    builder.put(setting.substring(prefix.length() + 1), nodeSettings.get(setting));
                }
            }
        }
        accounts = new HipChatAccounts(builder.build(), httpClient, logger);
    }
}
