/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.notification.pagerduty;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.watcher.support.http.HttpClient;
import org.elasticsearch.watcher.support.http.HttpRequest;
import org.elasticsearch.watcher.support.http.HttpResponse;
import org.elasticsearch.watcher.watch.Payload;

import java.io.IOException;

/**
 *
 */
public class PagerDutyAccount {

    public static final String SERVICE_KEY_SETTING = "service_api_key";
    public static final String TRIGGER_DEFAULTS_SETTING = "event_defaults";

    final String name;
    final String serviceKey;
    final HttpClient httpClient;
    final IncidentEventDefaults eventDefaults;
    final ESLogger logger;

    public PagerDutyAccount(String name, Settings accountSettings, Settings serviceSettings, HttpClient httpClient, ESLogger logger) {
        this.name = name;
        this.serviceKey = accountSettings.get(SERVICE_KEY_SETTING, serviceSettings.get(SERVICE_KEY_SETTING, null));
        if (this.serviceKey == null) {
            throw new SettingsException("invalid pagerduty account [" + name + "]. missing required [" + SERVICE_KEY_SETTING + "] setting");
        }
        this.httpClient = httpClient;

        this.eventDefaults = new IncidentEventDefaults(accountSettings.getAsSettings(TRIGGER_DEFAULTS_SETTING));
        this.logger = logger;
    }

    public String getName() {
        return name;
    }

    public IncidentEventDefaults getDefaults() {
        return eventDefaults;
    }

    public SentEvent send(IncidentEvent event, Payload payload) throws IOException {
        HttpRequest request = event.createRequest(serviceKey, payload);
        HttpResponse response = httpClient.execute(request);
        return SentEvent.responded(event, request, response);
    }
}
