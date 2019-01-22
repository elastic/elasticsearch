/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification.pagerduty;

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
 * A component to store pagerduty credentials.
 */
public class PagerDutyService extends NotificationService<PagerDutyAccount> {

    private static final Setting<String> SETTING_DEFAULT_ACCOUNT =
            Setting.simpleString("xpack.notification.pagerduty.default_account", Property.Dynamic, Property.NodeScope);

    private static final Setting.AffixSetting<SecureString> SETTING_SECURE_SERVICE_API_KEY =
            Setting.affixKeySetting("xpack.notification.pagerduty.account.", "secure_service_api_key",
                    (key) -> SecureSetting.secureString(key, null));

    private static final Setting.AffixSetting<Settings> SETTING_DEFAULTS =
            Setting.affixKeySetting("xpack.notification.pagerduty.account.", "event_defaults",
                    (key) -> Setting.groupSetting(key + ".", Property.Dynamic, Property.NodeScope));

    private final HttpClient httpClient;

    public PagerDutyService(Settings settings, HttpClient httpClient, ClusterSettings clusterSettings) {
        super("pagerduty", settings, clusterSettings, PagerDutyService.getDynamicSettings(), PagerDutyService.getSecureSettings());
        this.httpClient = httpClient;
        // ensure logging of setting changes
        clusterSettings.addSettingsUpdateConsumer(SETTING_DEFAULT_ACCOUNT, (s) -> {});
        clusterSettings.addAffixUpdateConsumer(SETTING_DEFAULTS, (s, o) -> {}, (s, o) -> {});
        // do an initial load
        reload(settings);
    }

    @Override
    protected PagerDutyAccount createAccount(String name, Settings accountSettings) {
        return new PagerDutyAccount(name, accountSettings, httpClient);
    }

    private static List<Setting<?>> getDynamicSettings() {
        return Arrays.asList(SETTING_DEFAULTS, SETTING_DEFAULT_ACCOUNT);
    }

    private static List<Setting<?>> getSecureSettings() {
        return Arrays.asList(SETTING_SECURE_SERVICE_API_KEY);
    }

    public static List<Setting<?>> getSettings() {
        List<Setting<?>> allSettings = new ArrayList<Setting<?>>(getDynamicSettings());
        allSettings.addAll(getSecureSettings());
        return allSettings;
    }
}
