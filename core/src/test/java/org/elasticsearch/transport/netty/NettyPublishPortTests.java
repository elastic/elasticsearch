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

package org.elasticsearch.transport.netty;

import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BindTransportException;
import org.elasticsearch.transport.TransportSettings;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import static java.net.InetAddress.getByName;
import static java.util.Arrays.asList;
import static org.elasticsearch.transport.netty.NettyTransport.resolvePublishPort;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class NettyPublishPortTests extends ESTestCase {

    public void testPublishPort() throws Exception {
        int boundPort = randomIntBetween(9000, 9100);
        int otherBoundPort = randomIntBetween(9200, 9300);

        boolean useProfile = randomBoolean();
        final String profile;
        final Settings settings;
        final Settings profileSettings;
        if (useProfile) {
            profile = "some_profile";
            settings = randomBoolean() ? Settings.EMPTY : Settings.builder().put(TransportSettings.PUBLISH_PORT.getKey(), 9081).build();
            profileSettings = Settings.builder().put("publish_port", 9080).build();
        } else {
            profile = TransportSettings.DEFAULT_PROFILE;
            settings = Settings.builder().put(TransportSettings.PUBLISH_PORT.getKey(), 9081).build();
            profileSettings = randomBoolean() ? Settings.EMPTY : Settings.builder().put("publish_port", 9080).build();;
        }

        int publishPort = resolvePublishPort(profile, settings, profileSettings,
            randomAddresses(), getByName("127.0.0.2"));
        assertThat("Publish port should be explicitly set", publishPort, equalTo(useProfile ? 9080 : 9081));

        publishPort = resolvePublishPort(profile, Settings.EMPTY, Settings.EMPTY,
            asList(address("127.0.0.1", boundPort), address("127.0.0.2", otherBoundPort)),
            getByName("127.0.0.1"));
        assertThat("Publish port should be derived from matched address", publishPort, equalTo(boundPort));

        publishPort = resolvePublishPort(profile, Settings.EMPTY, Settings.EMPTY,
            asList(address("127.0.0.1", boundPort), address("127.0.0.2", boundPort)),
            getByName("127.0.0.3"));
        assertThat("Publish port should be derived from unique port of bound addresses", publishPort, equalTo(boundPort));

        try {
            resolvePublishPort(profile, Settings.EMPTY, Settings.EMPTY,
                asList(address("127.0.0.1", boundPort), address("127.0.0.2", otherBoundPort)),
                getByName("127.0.0.3"));
            fail("Expected BindTransportException as publish_port not specified and non-unique port of bound addresses");
        } catch (BindTransportException e) {
            assertThat(e.getMessage(), containsString("Failed to auto-resolve publish port"));
        }

        publishPort = resolvePublishPort(profile, Settings.EMPTY, Settings.EMPTY,
            asList(address("0.0.0.0", boundPort), address("127.0.0.2", otherBoundPort)),
            getByName("127.0.0.1"));
        assertThat("Publish port should be derived from matching wildcard address", publishPort, equalTo(boundPort));

        if (NetworkUtils.SUPPORTS_V6) {
            publishPort = resolvePublishPort(profile, Settings.EMPTY, Settings.EMPTY,
                asList(address("0.0.0.0", boundPort), address("127.0.0.2", otherBoundPort)),
                getByName("::1"));
            assertThat("Publish port should be derived from matching wildcard address", publishPort, equalTo(boundPort));
        }
    }

    private InetSocketAddress address(String host, int port) throws UnknownHostException {
        return new InetSocketAddress(getByName(host), port);
    }

    private InetSocketAddress randomAddress() throws UnknownHostException {
        return address("127.0.0." + randomIntBetween(1, 100), randomIntBetween(9200, 9300));
    }

    private List<InetSocketAddress> randomAddresses() throws UnknownHostException {
        List<InetSocketAddress> addresses = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            addresses.add(randomAddress());
        }
        return addresses;
    }
}
