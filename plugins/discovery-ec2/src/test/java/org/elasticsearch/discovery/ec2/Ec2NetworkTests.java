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

package org.elasticsearch.discovery.ec2;

import org.elasticsearch.cloud.aws.network.Ec2NameResolver;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;

/**
 * Test for EC2 network.host settings.
 */
public class Ec2NetworkTests extends ESTestCase {

    /**
     * Test for network.host: _ec2_
     */
    @Test
    public void networkHostEc2() throws IOException {
        Settings nodeSettings = Settings.builder()
                .put("network.host", "_ec2_")
                .build();

        NetworkService networkService = new NetworkService(nodeSettings);
        networkService.addCustomNameResolver(new Ec2NameResolver(nodeSettings));
        // TODO we need to replace that with a mock. For now we check the URL we are supposed to reach.
        try {
            networkService.resolveBindHostAddress(null);
        } catch (IOException e) {
            assertThat(e.getMessage(), containsString("local-ipv4"));
        }
    }

    /**
     * Test for network.host: _ec2:publicIp_
     */
    @Test
    public void networkHostEc2PublicIp() throws IOException {
        Settings nodeSettings = Settings.builder()
                .put("network.host", "_ec2:publicIp_")
                .build();

        NetworkService networkService = new NetworkService(nodeSettings);
        networkService.addCustomNameResolver(new Ec2NameResolver(nodeSettings));
        // TODO we need to replace that with a mock. For now we check the URL we are supposed to reach.
        try {
            networkService.resolveBindHostAddress(null);
        } catch (IOException e) {
            assertThat(e.getMessage(), containsString("public-ipv4"));
        }
    }

    /**
     * Test for network.host: _ec2:privateIp_
     */
    @Test
    public void networkHostEc2PrivateIp() throws IOException {
        Settings nodeSettings = Settings.builder()
                .put("network.host", "_ec2:privateIp_")
                .build();

        NetworkService networkService = new NetworkService(nodeSettings);
        networkService.addCustomNameResolver(new Ec2NameResolver(nodeSettings));
        // TODO we need to replace that with a mock. For now we check the URL we are supposed to reach.
        try {
            networkService.resolveBindHostAddress(null);
        } catch (IOException e) {
            assertThat(e.getMessage(), containsString("local-ipv4"));
        }
    }

    /**
     * Test for network.host: _ec2:privateIpv4_
     */
    @Test
    public void networkHostEc2PrivateIpv4() throws IOException {
        Settings nodeSettings = Settings.builder()
                .put("network.host", "_ec2:privateIpv4_")
                .build();

        NetworkService networkService = new NetworkService(nodeSettings);
        networkService.addCustomNameResolver(new Ec2NameResolver(nodeSettings));
        // TODO we need to replace that with a mock. For now we check the URL we are supposed to reach.
        try {
            networkService.resolveBindHostAddress(null);
        } catch (IOException e) {
            assertThat(e.getMessage(), containsString("local-ipv4"));
        }
    }

    /**
     * Test for network.host: _ec2:privateDns_
     */
    @Test
    public void networkHostEc2PrivateDns() throws IOException {
        Settings nodeSettings = Settings.builder()
                .put("network.host", "_ec2:privateDns_")
                .build();

        NetworkService networkService = new NetworkService(nodeSettings);
        networkService.addCustomNameResolver(new Ec2NameResolver(nodeSettings));
        // TODO we need to replace that with a mock. For now we check the URL we are supposed to reach.
        try {
            networkService.resolveBindHostAddress(null);
        } catch (IOException e) {
            assertThat(e.getMessage(), containsString("local-hostname"));
        }
    }

    /**
     * Test for network.host: _ec2:publicIpv4_
     */
    @Test
    public void networkHostEc2PublicIpv4() throws IOException {
        Settings nodeSettings = Settings.builder()
                .put("network.host", "_ec2:publicIpv4_")
                .build();

        NetworkService networkService = new NetworkService(nodeSettings);
        networkService.addCustomNameResolver(new Ec2NameResolver(nodeSettings));
        // TODO we need to replace that with a mock. For now we check the URL we are supposed to reach.
        try {
            networkService.resolveBindHostAddress(null);
        } catch (IOException e) {
            assertThat(e.getMessage(), containsString("public-ipv4"));
        }
    }

    /**
     * Test for network.host: _ec2:publicDns_
     */
    @Test
    public void networkHostEc2PublicDns() throws IOException {
        Settings nodeSettings = Settings.builder()
                .put("network.host", "_ec2:publicDns_")
                .build();

        NetworkService networkService = new NetworkService(nodeSettings);
        networkService.addCustomNameResolver(new Ec2NameResolver(nodeSettings));
        // TODO we need to replace that with a mock. For now we check the URL we are supposed to reach.
        try {
            networkService.resolveBindHostAddress(null);
        } catch (IOException e) {
            assertThat(e.getMessage(), containsString("public-hostname"));
        }
    }

    /**
     * Test that we don't have any regression with network host core settings such as
     * network.host: _local_
     */
    @Test
    public void networkHostCoreLocal() throws IOException {
        Settings nodeSettings = Settings.builder()
                .put("network.host", "_local_")
                .build();

        NetworkService networkService = new NetworkService(nodeSettings);
        networkService.addCustomNameResolver(new Ec2NameResolver(nodeSettings));
        InetAddress[] addresses = networkService.resolveBindHostAddress(null);
        assertThat(addresses, arrayContaining(networkService.resolveBindHostAddress("_local_")));
    }
}
