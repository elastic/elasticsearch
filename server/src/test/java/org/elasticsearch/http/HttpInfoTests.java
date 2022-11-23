/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;

public class HttpInfoTests extends ESTestCase {

    public void testCorrectlyDisplayPublishedCname() throws Exception {
        InetAddress localhost = InetAddress.getByName("localhost");
        int port = 9200;
        assertPublishAddress(
            new HttpInfo(
                new BoundTransportAddress(
                    new TransportAddress[] { new TransportAddress(localhost, port) },
                    new TransportAddress(localhost, port)
                ),
                0L
            ),
            "localhost/" + NetworkAddress.format(localhost) + ':' + port
        );
    }

    public void testCorrectDisplayPublishedIp() throws Exception {
        InetAddress localhost = InetAddress.getByName(NetworkAddress.format(InetAddress.getByName("localhost")));
        int port = 9200;
        assertPublishAddress(
            new HttpInfo(
                new BoundTransportAddress(
                    new TransportAddress[] { new TransportAddress(localhost, port) },
                    new TransportAddress(localhost, port)
                ),
                0L
            ),
            NetworkAddress.format(localhost) + ':' + port
        );
    }

    public void testCorrectDisplayPublishedIpv6() throws Exception {
        int port = 9200;
        TransportAddress localhost = new TransportAddress(
            InetAddress.getByName(NetworkAddress.format(InetAddress.getByName("0:0:0:0:0:0:0:1"))),
            port
        );
        assertPublishAddress(
            new HttpInfo(new BoundTransportAddress(new TransportAddress[] { localhost }, localhost), 0L),
            localhost.toString()
        );
    }

    @SuppressWarnings("unchecked")
    private void assertPublishAddress(HttpInfo httpInfo, String expected) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        httpInfo.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals(
            expected,
            ((Map<String, Object>) createParser(builder).map().get(HttpInfo.Fields.HTTP)).get(HttpInfo.Fields.PUBLISH_ADDRESS)
        );
    }
}
