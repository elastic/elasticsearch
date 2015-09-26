/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.discovery.gce;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugin.discovery.gce.GceDiscoveryPlugin;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

public class GceDiscoverySettingsTests extends ESTestCase {
    public void testDiscoveryReady() {
        Settings settings = Settings.builder()
                .put("discovery.type", "gce")
                .put("cloud.gce.project_id", "gce_id")
                .putArray("cloud.gce.zone", "gce_zones_1", "gce_zones_2")
                .build();

        boolean discoveryReady = GceDiscoveryPlugin.isDiscoveryAlive(settings, logger);
        assertThat(discoveryReady, is(true));
    }

    public void testDiscoveryNotReady() {
        Settings settings = Settings.EMPTY;
        boolean discoveryReady = GceDiscoveryPlugin.isDiscoveryAlive(settings, logger);
        assertThat(discoveryReady, is(false));

        settings = Settings.builder()
                .put("discovery.type", "gce")
                .build();

        discoveryReady = GceDiscoveryPlugin.isDiscoveryAlive(settings, logger);
        assertThat(discoveryReady, is(false));

        settings = Settings.builder()
                .put("discovery.type", "gce")
                .put("cloud.gce.project_id", "gce_id")
                .build();

        discoveryReady = GceDiscoveryPlugin.isDiscoveryAlive(settings, logger);
        assertThat(discoveryReady, is(false));


        settings = Settings.builder()
                .put("discovery.type", "gce")
                .putArray("cloud.gce.zone", "gce_zones_1", "gce_zones_2")
                .build();

        discoveryReady = GceDiscoveryPlugin.isDiscoveryAlive(settings, logger);
        assertThat(discoveryReady, is(false));

        settings = Settings.builder()
                .put("cloud.gce.project_id", "gce_id")
                .putArray("cloud.gce.zone", "gce_zones_1", "gce_zones_2")
                .build();

        discoveryReady = GceDiscoveryPlugin.isDiscoveryAlive(settings, logger);
        assertThat(discoveryReady, is(false));
    }
}
