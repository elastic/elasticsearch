/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import static java.net.InetAddress.getByName;
import static java.util.Arrays.asList;
import static org.elasticsearch.transport.TcpTransport.resolvePublishPort;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class PublishPortTests extends ESTestCase {

    public void testPublishPort() throws Exception {
        int boundPort = randomIntBetween(9000, 9100);
        int otherBoundPort = randomIntBetween(9200, 9300);

        boolean useProfile = randomBoolean();
        final String profile;
        Settings baseSettings;
        Settings settings;
        if (useProfile) {
            baseSettings = Settings.builder().put("transport.profiles.some_profile.port", 0).build();
            settings = randomBoolean() ? Settings.EMPTY : Settings.builder().put(TransportSettings.PUBLISH_PORT.getKey(), 9081).build();
            settings = Settings.builder().put(settings).put(baseSettings).put("transport.profiles.some_profile.publish_port", 9080).build();
            profile = "some_profile";

        } else {
            baseSettings = Settings.EMPTY;
            settings = Settings.builder().put(TransportSettings.PUBLISH_PORT.getKey(), 9081).build();
            settings = randomBoolean() ? settings  :
                Settings.builder().put(settings).put("transport.profiles.default.publish_port", 9080).build();
            profile = "default";

        }

        int publishPort = resolvePublishPort(new TcpTransport.ProfileSettings(settings, profile),
            randomAddresses(), getByName("127.0.0.2"));
        assertThat("Publish port should be explicitly set", publishPort, equalTo(useProfile ? 9080 : 9081));

        publishPort = resolvePublishPort(new TcpTransport.ProfileSettings(baseSettings, profile),
            asList(address("127.0.0.1", boundPort), address("127.0.0.2", otherBoundPort)),
            getByName("127.0.0.1"));
        assertThat("Publish port should be derived from matched address", publishPort, equalTo(boundPort));

        publishPort = resolvePublishPort(new TcpTransport.ProfileSettings(baseSettings, profile),
            asList(address("127.0.0.1", boundPort), address("127.0.0.2", boundPort)),
            getByName("127.0.0.3"));
        assertThat("Publish port should be derived from unique port of bound addresses", publishPort, equalTo(boundPort));

        try {
            resolvePublishPort(new TcpTransport.ProfileSettings(baseSettings, profile),
                asList(address("127.0.0.1", boundPort), address("127.0.0.2", otherBoundPort)),
                getByName("127.0.0.3"));
            fail("Expected BindTransportException as publish_port not specified and non-unique port of bound addresses");
        } catch (BindTransportException e) {
            assertThat(e.getMessage(), containsString("Failed to auto-resolve publish port"));
        }

        publishPort = resolvePublishPort(new TcpTransport.ProfileSettings(baseSettings, profile),
            asList(address("0.0.0.0", boundPort), address("127.0.0.2", otherBoundPort)),
            getByName("127.0.0.1"));
        assertThat("Publish port should be derived from matching wildcard address", publishPort, equalTo(boundPort));

        if (NetworkUtils.SUPPORTS_V6) {
            publishPort = resolvePublishPort(new TcpTransport.ProfileSettings(baseSettings, profile),
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
