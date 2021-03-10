/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Map;

public class TransportInfoTests extends ESTestCase {

    private TransportInfo createTransportInfo(InetAddress address, int port, boolean cnameInPublishAddressProperty) {
        BoundTransportAddress boundAddress = new BoundTransportAddress(
                new TransportAddress[]{new TransportAddress(address, port)},
                new TransportAddress(address, port)
        );
        Map<String, BoundTransportAddress> profiles = Collections.singletonMap("test_profile", boundAddress);
        return new TransportInfo(boundAddress, profiles, cnameInPublishAddressProperty);
    }

    public void testDoNotForgetToRemoveProperty() {
        assertTrue("Remove es.transport.cname_in_publish_address property from TransportInfo in 9.0.0", Version.CURRENT.major < 9);
    }

    public void testCorrectlyDisplayPublishedCname() throws Exception {
        InetAddress address = InetAddress.getByName("localhost");
        int port = 9200;
        assertPublishAddress(
            createTransportInfo(address, port, false),
            "localhost/" + NetworkAddress.format(address) + ':' + port
        );
    }

    public void testDeprecatedWarningIfPropertySpecified() throws Exception {
        InetAddress address = InetAddress.getByName("localhost");
        int port = 9200;
        assertPublishAddress(
                createTransportInfo(address, port, true),
                "localhost/" + NetworkAddress.format(address) + ':' + port
        );
        assertWarnings("es.transport.cname_in_publish_address system property is deprecated and no longer affects " +
                        "transport.publish_address formatting. Remove this property to get rid of this deprecation warning.",

                "es.transport.cname_in_publish_address system property is deprecated and no longer affects " +
                        "transport.test_profile.publish_address formatting. Remove this property to get rid of this deprecation warning.");
    }

    public void testCorrectDisplayPublishedIp() throws Exception {
        InetAddress address = InetAddress.getByName(NetworkAddress.format(InetAddress.getByName("localhost")));
        int port = 9200;
        assertPublishAddress(
                createTransportInfo(address, port, false),
                NetworkAddress.format(address) + ':' + port
        );
    }

    public void testCorrectDisplayPublishedIpv6() throws Exception {
        InetAddress address = InetAddress.getByName(NetworkAddress.format(InetAddress.getByName("0:0:0:0:0:0:0:1")));
        int port = 9200;
        assertPublishAddress(
                createTransportInfo(address, port, false),
                new TransportAddress(address, port).toString()
        );
    }

    @SuppressWarnings("unchecked")
    private void assertPublishAddress(TransportInfo httpInfo, String expected) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        httpInfo.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        Map<String, Object> transportMap = (Map<String, Object>) createParser(builder).map().get(TransportInfo.Fields.TRANSPORT);
        Map<String, Object> profilesMap = (Map<String, Object>) transportMap.get("profiles");
        assertEquals(
            expected,
            transportMap.get(TransportInfo.Fields.PUBLISH_ADDRESS)
        );
        assertEquals(
                expected,
                ((Map<String, Object>)profilesMap.get("test_profile")).get(TransportInfo.Fields.PUBLISH_ADDRESS)
        );
    }
}
