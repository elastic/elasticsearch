/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.pagerduty;

import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.elasticsearch.xpack.watcher.common.http.HttpRequest;
import org.elasticsearch.xpack.watcher.common.http.HttpResponse;

import java.io.IOException;

public class PagerDutyAccount {

    private static final String TRIGGER_DEFAULTS_SETTING = "event_defaults";
    public static final Setting<SecureString> SECURE_SERVICE_API_KEY_SETTING = SecureSetting.secureString("secure_service_api_key", null);

    private final String name;
    private final String serviceKey;
    private final HttpClient httpClient;
    private final IncidentEventDefaults eventDefaults;

    PagerDutyAccount(String name, Settings accountSettings, HttpClient httpClient) {
        this.name = name;
        this.serviceKey = getServiceKey(name, accountSettings);
        this.httpClient = httpClient;

        this.eventDefaults = new IncidentEventDefaults(accountSettings.getAsSettings(TRIGGER_DEFAULTS_SETTING));
    }

    public String getName() {
        return name;
    }

    public IncidentEventDefaults getDefaults() {
        return eventDefaults;
    }

    public SentEvent send(IncidentEvent event, Payload payload, String watchId) throws IOException {
        HttpRequest request = event.createRequest(serviceKey, payload, watchId);
        HttpResponse response = httpClient.execute(request);
        return SentEvent.responded(event, request, response);
    }

    private static String getServiceKey(String name, Settings accountSettings) {
        SecureString secureString = SECURE_SERVICE_API_KEY_SETTING.get(accountSettings);
        if (secureString == null || secureString.length() < 1) {
            throw new SettingsException(
                "invalid pagerduty account [" + name + "]. missing required [" + SECURE_SERVICE_API_KEY_SETTING.getKey() + "] setting"
            );
        }
        return secureString.toString();
    }
}
