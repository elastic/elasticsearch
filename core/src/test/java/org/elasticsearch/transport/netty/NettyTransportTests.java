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

import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;

/** Unit tests for NettyTransport */
public class NettyTransportTests extends ESTestCase {
    
    /** Test ipv4 host with a default port works */
    public void testParseV4DefaultPort() throws Exception {
        TransportAddress[] addresses = NettyTransport.parse("127.0.0.1", "1234", Integer.MAX_VALUE);
        assertEquals(1, addresses.length);

        assertEquals("127.0.0.1", addresses[0].getAddress());
        assertEquals(1234, addresses[0].getPort());
    }

    /** Test ipv4 host with a default port range works */
    public void testParseV4DefaultRange() throws Exception {
        TransportAddress[] addresses = NettyTransport.parse("127.0.0.1", "1234-1235", Integer.MAX_VALUE);
        assertEquals(2, addresses.length);

        assertEquals("127.0.0.1", addresses[0].getAddress());
        assertEquals(1234, addresses[0].getPort());
        
        assertEquals("127.0.0.1", addresses[1].getAddress());
        assertEquals(1235, addresses[1].getPort());
    }

    /** Test ipv4 host with port works */
    public void testParseV4WithPort() throws Exception {
        TransportAddress[] addresses = NettyTransport.parse("127.0.0.1:2345", "1234", Integer.MAX_VALUE);
        assertEquals(1, addresses.length);

        assertEquals("127.0.0.1", addresses[0].getAddress());
        assertEquals(2345, addresses[0].getPort());
    }

    /** Test ipv4 host with port range works */
    public void testParseV4WithPortRange() throws Exception {
        TransportAddress[] addresses = NettyTransport.parse("127.0.0.1:2345-2346", "1234", Integer.MAX_VALUE);
        assertEquals(2, addresses.length);

        assertEquals("127.0.0.1", addresses[0].getAddress());
        assertEquals(2345, addresses[0].getPort());

        assertEquals("127.0.0.1", addresses[1].getAddress());
        assertEquals(2346, addresses[1].getPort());
    }

    /** Test unbracketed ipv6 hosts in configuration fail. Leave no ambiguity */
    public void testParseV6UnBracketed() throws Exception {
        try {
            NettyTransport.parse("::1", "1234", Integer.MAX_VALUE);
            fail("should have gotten exception");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("must be bracketed"));
        }
    }

    /** Test ipv6 host with a default port works */
    public void testParseV6DefaultPort() throws Exception {
        TransportAddress[] addresses = NettyTransport.parse("[::1]", "1234", Integer.MAX_VALUE);
        assertEquals(1, addresses.length);

        assertEquals("::1", addresses[0].getAddress());
        assertEquals(1234, addresses[0].getPort());
    }

    /** Test ipv6 host with a default port range works */
    public void testParseV6DefaultRange() throws Exception {
        TransportAddress[] addresses = NettyTransport.parse("[::1]", "1234-1235", Integer.MAX_VALUE);
        assertEquals(2, addresses.length);

        assertEquals("::1", addresses[0].getAddress());
        assertEquals(1234, addresses[0].getPort());
        
        assertEquals("::1", addresses[1].getAddress());
        assertEquals(1235, addresses[1].getPort());
    }

    /** Test ipv6 host with port works */
    public void testParseV6WithPort() throws Exception {
        TransportAddress[] addresses = NettyTransport.parse("[::1]:2345", "1234", Integer.MAX_VALUE);
        assertEquals(1, addresses.length);

        assertEquals("::1", addresses[0].getAddress());
        assertEquals(2345, addresses[0].getPort());
    }

    /** Test ipv6 host with port range works */
    public void testParseV6WithPortRange() throws Exception {
        TransportAddress[] addresses = NettyTransport.parse("[::1]:2345-2346", "1234", Integer.MAX_VALUE);
        assertEquals(2, addresses.length);

        assertEquals("::1", addresses[0].getAddress());
        assertEquals(2345, addresses[0].getPort());

        assertEquals("::1", addresses[1].getAddress());
        assertEquals(2346, addresses[1].getPort());
    }
    
    /** Test per-address limit */
    public void testAddressLimit() throws Exception {
        TransportAddress[] addresses = NettyTransport.parse("[::1]:100-200", "1000", 3);
        assertEquals(3, addresses.length);
        assertEquals(100, addresses[0].getPort());
        assertEquals(101, addresses[1].getPort());
        assertEquals(102, addresses[2].getPort());
    }
}
