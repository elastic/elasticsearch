/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.jira;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.elasticsearch.xpack.watcher.notification.NotificationService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A component to store Atlassian's JIRA credentials.
 *
 * https://www.atlassian.com/software/jira
 */
public class JiraService extends NotificationService<JiraAccount> {

    private static final Setting<String> SETTING_DEFAULT_ACCOUNT = Setting.simpleString(
        "xpack.notification.jira.default_account",
        Property.Dynamic,
        Property.NodeScope
    );

    private static final Setting.AffixSetting<Boolean> SETTING_ALLOW_HTTP = Setting.affixKeySetting(
        "xpack.notification.jira.account.",
        "allow_http",
        (key) -> Setting.boolSetting(key, false, Property.Dynamic, Property.NodeScope)
    );

    private static final Setting.AffixSetting<SecureString> SETTING_SECURE_USER = Setting.affixKeySetting(
        "xpack.notification.jira.account.",
        "secure_user",
        (key) -> SecureSetting.secureString(key, null)
    );

    private static final Setting.AffixSetting<SecureString> SETTING_SECURE_URL = Setting.affixKeySetting(
        "xpack.notification.jira.account.",
        "secure_url",
        (key) -> SecureSetting.secureString(key, null)
    );

    private static final Setting.AffixSetting<SecureString> SETTING_SECURE_PASSWORD = Setting.affixKeySetting(
        "xpack.notification.jira.account.",
        "secure_password",
        (key) -> SecureSetting.secureString(key, null)
    );

    private static final Setting.AffixSetting<Settings> SETTING_DEFAULTS = Setting.affixKeySetting(
        "xpack.notification.jira.account.",
        "issue_defaults",
        (key) -> Setting.groupSetting(key + ".", Property.Dynamic, Property.NodeScope)
    );

    private final HttpClient httpClient;

    public JiraService(Settings settings, HttpClient httpClient, ClusterSettings clusterSettings) {
        super("jira", settings, clusterSettings, JiraService.getDynamicSettings(), JiraService.getSecureSettings());
        this.httpClient = httpClient;
        // ensure logging of setting changes
        clusterSettings.addSettingsUpdateConsumer(SETTING_DEFAULT_ACCOUNT, (s) -> {});
        clusterSettings.addAffixUpdateConsumer(SETTING_ALLOW_HTTP, (s, o) -> {}, (s, o) -> {});
        clusterSettings.addAffixUpdateConsumer(SETTING_DEFAULTS, (s, o) -> {}, (s, o) -> {});
        // do an initial load
        reload(settings);
    }

    @Override
    protected JiraAccount createAccount(String name, Settings settings) {
        return new JiraAccount(name, settings, httpClient);
    }

    private static List<Setting<?>> getDynamicSettings() {
        return Arrays.asList(SETTING_DEFAULT_ACCOUNT, SETTING_ALLOW_HTTP, SETTING_DEFAULTS);
    }

    private static List<Setting<?>> getSecureSettings() {
        return Arrays.asList(SETTING_SECURE_USER, SETTING_SECURE_PASSWORD, SETTING_SECURE_URL);
    }

    public static List<Setting<?>> getSettings() {
        List<Setting<?>> allSettings = new ArrayList<Setting<?>>(getDynamicSettings());
        allSettings.addAll(getSecureSettings());
        return allSettings;
    }
}
