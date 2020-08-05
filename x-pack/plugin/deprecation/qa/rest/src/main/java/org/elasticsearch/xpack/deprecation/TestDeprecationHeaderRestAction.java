/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Enables testing {@code DeprecationRestHandler} via integration tests by guaranteeing a deprecated REST endpoint.
 * <p>
 * This adds an endpoint named <code>/_test_cluster/deprecated_settings</code> that touches specified settings given their names
 * and returns their values.
 */
public class TestDeprecationHeaderRestAction extends BaseRestHandler {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(TestDeprecationHeaderRestAction.class);

    public static final Setting<Boolean> TEST_DEPRECATED_SETTING_TRUE1 =
        Setting.boolSetting("test.setting.deprecated.true1", true,
                            Setting.Property.NodeScope, Setting.Property.Deprecated, Setting.Property.Dynamic);
    public static final Setting<Boolean> TEST_DEPRECATED_SETTING_TRUE2 =
        Setting.boolSetting("test.setting.deprecated.true2", true,
                            Setting.Property.NodeScope, Setting.Property.Deprecated, Setting.Property.Dynamic);
    public static final Setting<Boolean> TEST_NOT_DEPRECATED_SETTING =
        Setting.boolSetting("test.setting.not_deprecated", false,
                            Setting.Property.NodeScope, Setting.Property.Dynamic);

    private static final Map<String, Setting<?>> SETTINGS_MAP = Map.of(
        TEST_DEPRECATED_SETTING_TRUE1.getKey(), TEST_DEPRECATED_SETTING_TRUE1,
        TEST_DEPRECATED_SETTING_TRUE2.getKey(), TEST_DEPRECATED_SETTING_TRUE2,
        TEST_NOT_DEPRECATED_SETTING.getKey(), TEST_NOT_DEPRECATED_SETTING);

    public static final String DEPRECATED_ENDPOINT = "[/_test_cluster/deprecated_settings] exists for deprecated tests";
    public static final String DEPRECATED_USAGE = "[deprecated_settings] usage is deprecated. use [settings] instead";

    private final Settings settings;

    public TestDeprecationHeaderRestAction(Settings settings) {
        this.settings = settings;
    }

    @Override
    public List<DeprecatedRoute> deprecatedRoutes() {
        return singletonList(
            new DeprecatedRoute(GET, "/_test_cluster/deprecated_settings", DEPRECATED_ENDPOINT));
    }

    @Override
    public String getName() {
        return "test_deprecation_header_action";
    }

    @Override
    public List<Route> routes() {
        return Collections.emptyList();
    }

    @SuppressWarnings("unchecked") // List<String> casts
    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final List<String> settings;

        try (XContentParser parser = request.contentParser()) {
            final Map<String, Object> source = parser.map();

            if (source.containsKey("deprecated_settings")) {
                deprecationLogger.deprecate("deprecated_settings", DEPRECATED_USAGE);

                settings = (List<String>)source.get("deprecated_settings");
            } else {
                settings = (List<String>)source.get("settings");
            }
        }

        return channel -> {
            final XContentBuilder builder = channel.newBuilder();

            builder.startObject().startArray("settings");
            for (String setting : settings) {
                builder.startObject().field(setting, SETTINGS_MAP.get(setting).get(this.settings)).endObject();
            }
            builder.endArray().endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        };
    }
}
