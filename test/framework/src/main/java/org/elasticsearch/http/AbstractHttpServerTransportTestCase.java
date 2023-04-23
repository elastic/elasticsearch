/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.http;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.netty4.HttpHeadersUtils;
import org.elasticsearch.http.netty4.Netty4HttpHeaderValidator;
import org.elasticsearch.test.ESTestCase;

import java.util.function.Supplier;

public class AbstractHttpServerTransportTestCase extends ESTestCase {

    /**
     * Trivial {@link HttpHeadersUtils} implementation that successfully validates any and all HTTP request headers.
     */
    public static final Supplier<Netty4HttpHeaderValidator> VALIDATE_EVERYTHING_VALIDATOR = () -> HttpHeadersUtils
        .getValidatorInboundHandler((httpPreRequest, channel, listener) -> listener.onResponse(null), new ThreadContext(Settings.EMPTY));

    protected static ClusterSettings randomClusterSettings() {
        return new ClusterSettings(
            Settings.builder().put(HttpTransportSettings.SETTING_HTTP_CLIENT_STATS_ENABLED.getKey(), randomBoolean()).build(),
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS
        );
    }
}
