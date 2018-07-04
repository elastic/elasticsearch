/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification.slack;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.elasticsearch.xpack.watcher.notification.NotificationService;

import java.util.Arrays;
import java.util.List;

/**
 * A component to store slack credentials.
 */
public class SlackService extends NotificationService<SlackAccount> {

    private static final Setting<String> SETTING_DEFAULT_ACCOUNT =
            Setting.simpleString("xpack.notification.slack.default_account", Property.Dynamic, Property.NodeScope);

    private static final Setting.AffixSetting<String> SETTING_URL =
            Setting.affixKeySetting("xpack.notification.slack.account.", "url",
                    (key) -> Setting.simpleString(key, Property.Dynamic, Property.NodeScope, Property.Filtered, Property.Deprecated));

    private static final Setting.AffixSetting<String> SETTING_URL_SECURE =
            Setting.affixKeySetting("xpack.notification.slack.account.", "secure_url",
                    (key) -> SecureSetting.simpleString(key, Property.Dynamic, Property.NodeScope, Property.Filtered));

    private static final Setting.AffixSetting<Settings> SETTING_DEFAULTS =
            Setting.affixKeySetting("xpack.notification.slack.account.", "message_defaults",
                    (key) -> Setting.groupSetting(key + ".", Property.Dynamic, Property.NodeScope));

    private final HttpClient httpClient;

    public SlackService(Settings settings, HttpClient httpClient, ClusterSettings clusterSettings) {
        super(settings, "slack", clusterSettings, SlackService.getSettings());
        this.httpClient = httpClient;
        clusterSettings.addSettingsUpdateConsumer(SETTING_DEFAULT_ACCOUNT, (s) -> {});
        clusterSettings.addAffixUpdateConsumer(SETTING_URL, (s, o) -> {}, (s, o) -> {});
        clusterSettings.addAffixUpdateConsumer(SETTING_URL_SECURE, (s, o) -> {}, (s, o) -> {});
        clusterSettings.addAffixUpdateConsumer(SETTING_DEFAULTS, (s, o) -> {}, (s, o) -> {});
        setAccountSetting(settings);
    }

    @Override
    protected SlackAccount createAccount(String name, Settings accountSettings) {
        return new SlackAccount(name, accountSettings, accountSettings, httpClient, logger);
    }

    public static List<Setting<?>> getSettings() {
        return Arrays.asList(SETTING_URL, SETTING_URL_SECURE, SETTING_DEFAULT_ACCOUNT, SETTING_DEFAULTS);
    }
}
