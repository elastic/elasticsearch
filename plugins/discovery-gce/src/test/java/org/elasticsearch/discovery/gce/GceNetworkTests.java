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

import org.elasticsearch.cloud.gce.network.GceNameResolver;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;

/**
 * Test for GCE network.host settings.
 * Related to https://github.com/elastic/elasticsearch/issues/13605
 */
public class GceNetworkTests extends ESTestCase {
    /**
     * Test for network.host: _gce_
     */
    public void testNetworkHostGceDefault() throws IOException {
        resolveGce("_gce_", InetAddress.getByName("10.240.0.2"));
    }

    /**
     * Test for network.host: _gce:privateIp_
     */
    public void testNetworkHostPrivateIp() throws IOException {
        resolveGce("_gce:privateIp_", InetAddress.getByName("10.240.0.2"));
    }

    /**
     * Test for network.host: _gce:hostname_
     */
    public void testNetworkHostPrivateDns() throws IOException {
        resolveGce("_gce:hostname_", InetAddress.getByName("localhost"));
    }

    /**
     * Test for network.host: _gce:doesnotexist_
     * This should raise an IllegalArgumentException as this setting does not exist
     */
    public void testNetworkHostWrongSetting() throws IOException {
        resolveGce("_gce:doesnotexist_", (InetAddress) null);
    }

    /**
     * Test with multiple network interfaces:
     * network.host: _gce:privateIp:0_
     * network.host: _gce:privateIp:1_
     */
    public void testNetworkHostPrivateIpInterface() throws IOException {
        resolveGce("_gce:privateIp:0_", InetAddress.getByName("10.240.0.2"));
        resolveGce("_gce:privateIp:1_", InetAddress.getByName("10.150.0.1"));
    }

    /**
     * Test that we don't have any regression with network host core settings such as
     * network.host: _local_
     */
    public void networkHostCoreLocal() throws IOException {
        resolveGce("_local_", new NetworkService(Settings.EMPTY, Collections.emptyList())
            .resolveBindHostAddresses(new String[] { NetworkService.DEFAULT_NETWORK_HOST }));
    }

    /**
     * Utility test method to test different settings
     * @param gceNetworkSetting tested network.host property
     * @param expected expected InetAddress, null if we expect an exception
     * @throws IOException Well... If something goes wrong :)
     */
    private void resolveGce(String gceNetworkSetting, InetAddress expected) throws IOException {
        resolveGce(gceNetworkSetting, expected == null ? null : new InetAddress [] { expected });
    }

    /**
     * Utility test method to test different settings
     * @param gceNetworkSetting tested network.host property
     * @param expected expected InetAddress, null if we expect an exception
     * @throws IOException Well... If something goes wrong :)
     */
    private void resolveGce(String gceNetworkSetting, InetAddress[] expected) throws IOException {
        Settings nodeSettings = Settings.builder()
                .put("network.host", gceNetworkSetting)
                .build();

        GceMetadataServiceMock mock = new GceMetadataServiceMock(nodeSettings);
        NetworkService networkService = new NetworkService(nodeSettings, Collections.singletonList(new GceNameResolver(nodeSettings, mock)));
        try {
            InetAddress[] addresses = networkService.resolveBindHostAddresses(null);
            if (expected == null) {
                fail("We should get a IllegalArgumentException when setting network.host: _gce:doesnotexist_");
            }
            assertThat(addresses, arrayContaining(expected));
        } catch (IllegalArgumentException e) {
            if (expected != null) {
                // We were expecting something and not an exception
                throw e;
            }
            // We check that we get the expected exception
            assertThat(e.getMessage(), containsString("is not one of the supported GCE network.host setting"));
        }
    }
}
