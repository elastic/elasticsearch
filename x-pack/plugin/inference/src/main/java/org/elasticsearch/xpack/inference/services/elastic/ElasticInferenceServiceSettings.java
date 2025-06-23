/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;

import java.util.ArrayList;
import java.util.List;

/**
 * Encapsulates settings using {@link Setting}. This does not represent service settings that are persisted
 * via {@link org.elasticsearch.inference.ServiceSettings}, but rather Elasticsearch settings passed on startup.
 */
public class ElasticInferenceServiceSettings {

    public static final String ELASTIC_INFERENCE_SERVICE_SSL_CONFIGURATION_PREFIX = "xpack.inference.elastic.http.ssl.";

    @Deprecated
    static final Setting<String> EIS_GATEWAY_URL = Setting.simpleString("xpack.inference.eis.gateway.url", Setting.Property.NodeScope);

    static final Setting<String> ELASTIC_INFERENCE_SERVICE_URL = Setting.simpleString(
        "xpack.inference.elastic.url",
        Setting.Property.NodeScope
    );

    /**
     * This setting is for testing only. It controls whether authorization is only performed once at bootup. If set to true, an
     * authorization request will be made repeatedly on an interval.
     */
    static final Setting<Boolean> PERIODIC_AUTHORIZATION_ENABLED = Setting.boolSetting(
        "xpack.inference.elastic.periodic_authorization_enabled",
        true,
        Setting.Property.NodeScope
    );

    private static final TimeValue DEFAULT_AUTH_REQUEST_INTERVAL = TimeValue.timeValueMinutes(10);
    static final Setting<TimeValue> AUTHORIZATION_REQUEST_INTERVAL = Setting.timeSetting(
        "xpack.inference.elastic.authorization_request_interval",
        DEFAULT_AUTH_REQUEST_INTERVAL,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final TimeValue DEFAULT_AUTH_REQUEST_JITTER = TimeValue.timeValueMinutes(5);
    static final Setting<TimeValue> MAX_AUTHORIZATION_REQUEST_JITTER = Setting.timeSetting(
        "xpack.inference.elastic.max_authorization_request_jitter",
        DEFAULT_AUTH_REQUEST_JITTER,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final SSLConfigurationSettings ELASTIC_INFERENCE_SERVICE_SSL_CONFIGURATION_SETTINGS = SSLConfigurationSettings.withPrefix(
        ELASTIC_INFERENCE_SERVICE_SSL_CONFIGURATION_PREFIX,
        false
    );

    public static final Setting<Boolean> ELASTIC_INFERENCE_SERVICE_SSL_ENABLED = Setting.boolSetting(
        ELASTIC_INFERENCE_SERVICE_SSL_CONFIGURATION_PREFIX + "enabled",
        true,
        Setting.Property.NodeScope
    );

    /**
     * Total time to live (TTL)  defines maximum life span of persistent connections regardless of their
     * expiration setting. No persistent connection will be re-used past its TTL value.
     * Using a TTL of -1 will disable the expiration of persistent connections (the idle connection evictor will still apply).
     */
    public static final Setting<TimeValue> CONNECTION_TTL_SETTING = Setting.timeSetting(
        "xpack.inference.elastic.http.connection_ttl",
        TimeValue.timeValueSeconds(60),
        Setting.Property.NodeScope
    );

    @Deprecated
    private final String eisGatewayUrl;

    private final String elasticInferenceServiceUrl;
    private final boolean periodicAuthorizationEnabled;
    private volatile TimeValue authRequestInterval;
    private volatile TimeValue maxAuthorizationRequestJitter;
    private final TimeValue connectionTtl;

    public ElasticInferenceServiceSettings(Settings settings) {
        eisGatewayUrl = EIS_GATEWAY_URL.get(settings);
        elasticInferenceServiceUrl = ELASTIC_INFERENCE_SERVICE_URL.get(settings);
        periodicAuthorizationEnabled = PERIODIC_AUTHORIZATION_ENABLED.get(settings);
        authRequestInterval = AUTHORIZATION_REQUEST_INTERVAL.get(settings);
        maxAuthorizationRequestJitter = MAX_AUTHORIZATION_REQUEST_JITTER.get(settings);
        connectionTtl = CONNECTION_TTL_SETTING.get(settings);
    }

    /**
     * This must be called after the object is constructed to avoid leaking the this reference before the constructor
     * finishes.
     *
     * Handles initializing the settings changes listener.
     */
    public final void init(ClusterService clusterService) {
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(AUTHORIZATION_REQUEST_INTERVAL, this::setAuthorizationRequestInterval);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(MAX_AUTHORIZATION_REQUEST_JITTER, this::setMaxAuthorizationRequestJitter);
    }

    private void setAuthorizationRequestInterval(TimeValue interval) {
        authRequestInterval = interval;
    }

    private void setMaxAuthorizationRequestJitter(TimeValue jitter) {
        maxAuthorizationRequestJitter = jitter;
    }

    public TimeValue getAuthRequestInterval() {
        return authRequestInterval;
    }

    public TimeValue getMaxAuthorizationRequestJitter() {
        return maxAuthorizationRequestJitter;
    }

    public TimeValue getConnectionTtl() {
        return connectionTtl;
    }

    public static List<Setting<?>> getSettingsDefinitions() {
        ArrayList<Setting<?>> settings = new ArrayList<>();
        settings.add(EIS_GATEWAY_URL);
        settings.add(ELASTIC_INFERENCE_SERVICE_URL);
        settings.add(ELASTIC_INFERENCE_SERVICE_SSL_ENABLED);
        settings.addAll(ELASTIC_INFERENCE_SERVICE_SSL_CONFIGURATION_SETTINGS.getEnabledSettings());
        settings.add(PERIODIC_AUTHORIZATION_ENABLED);
        settings.add(AUTHORIZATION_REQUEST_INTERVAL);
        settings.add(MAX_AUTHORIZATION_REQUEST_JITTER);
        settings.add(CONNECTION_TTL_SETTING);
        return settings;
    }

    public String getElasticInferenceServiceUrl() {
        return Strings.isEmpty(elasticInferenceServiceUrl) ? eisGatewayUrl : elasticInferenceServiceUrl;
    }

    public boolean isPeriodicAuthorizationEnabled() {
        return periodicAuthorizationEnabled;
    }
}
