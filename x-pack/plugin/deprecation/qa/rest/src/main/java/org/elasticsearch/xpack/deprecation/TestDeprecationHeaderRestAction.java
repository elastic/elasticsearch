/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Enables testing {@code DeprecationRestHandler} via integration tests by guaranteeing a deprecated REST endpoint.
 * <p>
 * This adds an endpoint named <code>/_test_cluster/deprecated_settings</code> that touches specified settings given their names
 * and returns their values.
 */
public class TestDeprecationHeaderRestAction extends BaseRestHandler {
    private static final Logger logger = LogManager.getLogger(TestDeprecationHeaderRestAction.class);
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(logger.getName());

    public static final Setting<Boolean> TEST_DEPRECATED_SETTING_TRUE1 = Setting.boolSetting(
        "test.setting.deprecated.true1",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Deprecated,
        Setting.Property.Dynamic
    );
    public static final Setting<Boolean> TEST_DEPRECATED_SETTING_TRUE2 = Setting.boolSetting(
        "test.setting.deprecated.true2",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Deprecated,
        Setting.Property.Dynamic
    );
    public static final Setting<Boolean> TEST_NOT_DEPRECATED_SETTING = Setting.boolSetting(
        "test.setting.not_deprecated",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final Map<String, Setting<?>> SETTINGS_MAP;

    static {
        Map<String, Setting<?>> settingsMap = new HashMap<>(3);

        settingsMap.put(TEST_DEPRECATED_SETTING_TRUE1.getKey(), TEST_DEPRECATED_SETTING_TRUE1);
        settingsMap.put(TEST_DEPRECATED_SETTING_TRUE2.getKey(), TEST_DEPRECATED_SETTING_TRUE2);
        settingsMap.put(TEST_NOT_DEPRECATED_SETTING.getKey(), TEST_NOT_DEPRECATED_SETTING);

        SETTINGS_MAP = Collections.unmodifiableMap(settingsMap);
    }

    public static final String DEPRECATED_ENDPOINT = "[/_test_cluster/deprecated_settings] exists for deprecated tests";
    public static final String DEPRECATED_USAGE = "[deprecated_settings] usage is deprecated. use [settings] instead";
    public static final String DEPRECATED_WARN_USAGE =
        "[deprecated_warn_settings] usage is deprecated but won't be breaking in next version";

    private final Settings settings;

    public TestDeprecationHeaderRestAction(Settings settings) {
        this.settings = settings;
    }

    @Override
    public String getName() {
        return "test_deprecation_header_action";
    }

    @Override
    public List<Route> routes() {
        return org.elasticsearch.core.List.of(
            // note: RestApiVersion.current() is acceptable here because this is test code -- ordinary callers of `.deprecated(...)`
            // should use an actual version
            Route.builder(GET, "/_test_cluster/deprecated_settings").deprecated(DEPRECATED_ENDPOINT, RestApiVersion.current()).build(),
            Route.builder(POST, "/_test_cluster/deprecated_settings").deprecated(DEPRECATED_ENDPOINT, RestApiVersion.current()).build()
        );
    }

    @SuppressWarnings("unchecked") // List<String> casts
    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final List<String> settings;

        try (XContentParser parser = request.contentParser()) {
            final Map<String, Object> source = parser.map();
            if (source.containsKey("deprecated_settings")) {
                deprecationLogger.critical(DeprecationCategory.SETTINGS, "deprecated_settings", DEPRECATED_USAGE);
                settings = (List<String>) source.get("deprecated_settings");
            } else if (source.containsKey("deprecation_warning")) {
                deprecationLogger.warn(DeprecationCategory.SETTINGS, "deprecated_warn_settings", DEPRECATED_WARN_USAGE);
                settings = (List<String>) source.get("deprecation_warning");
            } else {
                settings = (List<String>) source.get("settings");
            }
        }

        // Pull out the settings values here in order to guarantee that any deprecation messages are triggered
        // in the same thread context.
        final Map<String, Object> settingsMap = new HashMap<>();
        for (String setting : settings) {
            settingsMap.put(setting, SETTINGS_MAP.get(setting).get(this.settings));
        }

        return channel -> {
            final XContentBuilder builder = channel.newBuilder();

            builder.startObject().startArray("settings");
            for (Map.Entry<String, Object> entry : settingsMap.entrySet()) {
                builder.startObject().field(entry.getKey(), entry.getValue()).endObject();
            }
            builder.endArray().endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        };
    }
}
