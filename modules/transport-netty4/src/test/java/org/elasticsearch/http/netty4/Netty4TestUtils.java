/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.netty4;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.transport.netty4.Netty4Plugin;

import static org.elasticsearch.test.ESTestCase.randomBoolean;

public enum Netty4TestUtils {
    ;

    public static ClusterSettings randomClusterSettings() {
        return new ClusterSettings(
            Settings.builder().put(HttpTransportSettings.SETTING_HTTP_CLIENT_STATS_ENABLED.getKey(), randomBoolean()).build(),
            Sets.addToCopy(
                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
                Netty4Plugin.SETTING_HTTP_NETTY_TLS_HANDSHAKES_MAX_IN_PROGRESS,
                Netty4Plugin.SETTING_HTTP_NETTY_TLS_HANDSHAKES_MAX_DELAYED
            )
        );
    }
}
