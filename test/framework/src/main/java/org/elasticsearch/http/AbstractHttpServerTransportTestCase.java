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
import org.elasticsearch.test.ESTestCase;

public class AbstractHttpServerTransportTestCase extends ESTestCase {

    protected static ClusterSettings randomClusterSettings() {
        return new ClusterSettings(
            Settings.builder().put(HttpTransportSettings.SETTING_HTTP_CLIENT_STATS_ENABLED.getKey(), randomBoolean()).build(),
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS
        );
    }
}
