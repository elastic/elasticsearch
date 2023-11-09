/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.retry.RetrySettings;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderFactory;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class Utils {
    public static ClusterService mockClusterServiceEmpty() {
        return mockClusterService(Settings.EMPTY);
    }

    public static ClusterService mockClusterService(Settings settings) {
        var clusterService = mock(ClusterService.class);

        var registeredSettings = Stream.of(
            HttpSettings.getSettings(),
            HttpClientManager.getSettings(),
            HttpRequestSenderFactory.HttpRequestSender.getSettings(),
            ThrottlerManager.getSettings(),
            RetrySettings.getSettingsDefinitions()
        ).flatMap(Collection::stream).collect(Collectors.toSet());

        var cSettings = new ClusterSettings(settings, registeredSettings);
        when(clusterService.getClusterSettings()).thenReturn(cSettings);

        return clusterService;
    }

    public static String getUrl(MockWebServer webServer) {
        return format("http://%s:%s", webServer.getHostName(), webServer.getPort());
    }

    public static Map<String, Object> entityAsMap(String body) throws IOException {
        InputStream bodyStream = new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8));

        return entityAsMap(bodyStream);
    }

    public static Map<String, Object> entityAsMap(InputStream body) throws IOException {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(
                    XContentParserConfiguration.EMPTY.withRegistry(NamedXContentRegistry.EMPTY)
                        .withDeprecationHandler(DeprecationHandler.THROW_UNSUPPORTED_OPERATION),
                    body
                )
        ) {
            return parser.map();
        }
    }

    public static ScalingExecutorBuilder inferenceUtilityPool() {
        return new ScalingExecutorBuilder(
            UTILITY_THREAD_POOL_NAME,
            1,
            4,
            TimeValue.timeValueMinutes(10),
            false,
            "xpack.inference.utility_thread_pool"
        );
    }
}
