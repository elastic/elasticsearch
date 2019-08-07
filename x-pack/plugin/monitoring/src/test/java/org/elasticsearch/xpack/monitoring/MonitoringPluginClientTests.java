/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

public class MonitoringPluginClientTests extends ESTestCase {

    public void testModulesWithNodeSettings() throws Exception {
        // these settings mimic what ES does when running as a node...
        Settings settings = Settings.builder()
                .put("path.home", createTempDir())
                .put(Client.CLIENT_TYPE_SETTING_S.getKey(), "node")
                .build();
        Monitoring plugin = new Monitoring(settings);
        assertThat(plugin.isEnabled(), is(true));
    }
}
