/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification.jira;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.elasticsearch.xpack.watcher.notification.NotificationService;

/**
 * A component to store Atlassian's JIRA credentials.
 *
 * https://www.atlassian.com/software/jira
 */
public class JiraService extends NotificationService<JiraAccount> {

    public static final Setting<Settings> JIRA_ACCOUNT_SETTING =
            Setting.groupSetting("xpack.notification.jira.", Setting.Property.Dynamic, Setting.Property.NodeScope);

    private final HttpClient httpClient;

    public JiraService(Settings settings, HttpClient httpClient, ClusterSettings clusterSettings) {
        super(settings, "jira");
        this.httpClient = httpClient;
        clusterSettings.addSettingsUpdateConsumer(JIRA_ACCOUNT_SETTING, this::setAccountSetting);
        setAccountSetting(JIRA_ACCOUNT_SETTING.get(settings));
    }

    @Override
    protected JiraAccount createAccount(String name, Settings accountSettings) {
        return new JiraAccount(name, accountSettings, httpClient);
    }
}
