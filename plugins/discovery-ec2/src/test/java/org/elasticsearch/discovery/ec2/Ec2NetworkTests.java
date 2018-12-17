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

import com.sun.net.httpserver.HttpServer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.BiConsumer;

import static com.amazonaws.SDKGlobalConfiguration.EC2_METADATA_SERVICE_OVERRIDE_SYSTEM_PROPERTY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;

/**
 * Test for EC2 network.host settings.
 * <p>
 * Warning: This test doesn't assert that the exceptions are thrown.
 * They aren't.
 */
@SuppressForbidden(reason = "use http server")
public class Ec2NetworkTests extends ESTestCase {

    private static HttpServer httpServer;

    @BeforeClass
    public static void startHttp() throws Exception {
        httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress().getHostAddress(), 0), 0);

        BiConsumer<String, String> registerContext = (path, v) ->{
            final byte[] message = v.getBytes(UTF_8);
            httpServer.createContext(path, (s) -> {
                s.sendResponseHeaders(RestStatus.OK.getStatus(), message.length);
                OutputStream responseBody = s.getResponseBody();
                responseBody.write(message);
                responseBody.close();
            });
        };
        registerContext.accept("/latest/meta-data/local-ipv4","127.0.0.1");
        registerContext.accept("/latest/meta-data/public-ipv4","165.168.10.2");
        registerContext.accept("/latest/meta-data/public-hostname","165.168.10.3");
        registerContext.accept("/latest/meta-data/local-hostname","10.10.10.5");

        httpServer.start();
    }

    @Before
    public void setup() {
        // redirect EC2 metadata service to httpServer
        AccessController.doPrivileged((PrivilegedAction<String>) () -> System.setProperty(EC2_METADATA_SERVICE_OVERRIDE_SYSTEM_PROPERTY,
            "http://" + httpServer.getAddress().getHostName() + ":" + httpServer.getAddress().getPort()));
    }

    @AfterClass
    public static void stopHttp() {
        httpServer.stop(0);
        httpServer = null;
    }

    /**
     * Test for network.host: _ec2_
     */
    public void testNetworkHostEc2() throws IOException {
        resolveEc2("_ec2_", InetAddress.getByName("127.0.0.1"));
    }

    /**
     * Test for network.host: _ec2_
     */
    public void testNetworkHostUnableToResolveEc2() {
        // redirect EC2 metadata service to unknown location
        AccessController.doPrivileged((PrivilegedAction<String>) () -> System.setProperty(EC2_METADATA_SERVICE_OVERRIDE_SYSTEM_PROPERTY,
            "http://127.0.0.1/"));

        try {
            resolveEc2("_ec2_", (InetAddress[]) null);
        } catch (IOException e) {
            assertThat(e.getMessage(),
                equalTo("IOException caught when fetching InetAddress from [http://127.0.0.1//latest/meta-data/local-ipv4]"));
        }
    }

    /**
     * Test for network.host: _ec2:publicIp_
     */
    public void testNetworkHostEc2PublicIp() throws IOException {
        resolveEc2("_ec2:publicIp_", InetAddress.getByName("165.168.10.2"));
    }

    /**
     * Test for network.host: _ec2:privateIp_
     */
    public void testNetworkHostEc2PrivateIp() throws IOException {
        resolveEc2("_ec2:privateIp_", InetAddress.getByName("127.0.0.1"));
    }

    /**
     * Test for network.host: _ec2:privateIpv4_
     */
    public void testNetworkHostEc2PrivateIpv4() throws IOException {
        resolveEc2("_ec2:privateIpv4_", InetAddress.getByName("127.0.0.1"));
    }

    /**
     * Test for network.host: _ec2:privateDns_
     */
    public void testNetworkHostEc2PrivateDns() throws IOException {
        resolveEc2("_ec2:privateDns_", InetAddress.getByName("10.10.10.5"));
    }

    /**
     * Test for network.host: _ec2:publicIpv4_
     */
    public void testNetworkHostEc2PublicIpv4() throws IOException {
        resolveEc2("_ec2:publicIpv4_", InetAddress.getByName("165.168.10.2"));
    }

    /**
     * Test for network.host: _ec2:publicDns_
     */
    public void testNetworkHostEc2PublicDns() throws IOException {
        resolveEc2("_ec2:publicDns_", InetAddress.getByName("165.168.10.3"));
    }

    private InetAddress[] resolveEc2(String host, InetAddress ... expected) throws IOException {
        Settings nodeSettings = Settings.builder()
            .put("network.host", host)
            .build();

        NetworkService networkService = new NetworkService(Collections.singletonList(new Ec2NameResolver()));

        InetAddress[] addresses = networkService.resolveBindHostAddresses(
            NetworkService.GLOBAL_NETWORK_BIND_HOST_SETTING.get(nodeSettings).toArray(Strings.EMPTY_ARRAY));
        if (expected == null) {
            fail("We should get an IOException, resolved addressed:" + Arrays.toString(addresses));
        }
        assertThat(addresses, arrayContaining(expected));
        return addresses;
    }

    /**
     * Test that we don't have any regression with network host core settings such as
     * network.host: _local_
     */
    public void testNetworkHostCoreLocal() throws IOException {
        NetworkService networkService = new NetworkService(Collections.singletonList(new Ec2NameResolver()));
        InetAddress[] addresses = networkService.resolveBindHostAddresses(null);
        assertThat(addresses, arrayContaining(networkService.resolveBindHostAddresses(new String[] { "_local_" })));
    }
}
