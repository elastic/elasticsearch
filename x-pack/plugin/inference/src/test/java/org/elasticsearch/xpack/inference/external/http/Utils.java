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
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderFactory;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.core.Strings.format;
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
            HttpRequestSenderFactory.HttpRequestSender.getSettings()
        ).flatMap(Collection::stream).collect(Collectors.toSet());

        var cSettings = new ClusterSettings(settings, registeredSettings);
        when(clusterService.getClusterSettings()).thenReturn(cSettings);

        return clusterService;
    }

    public static String getUrl(MockWebServer webServer) {
        return format("http://%s:%s", webServer.getHostName(), webServer.getPort());
    }
}
