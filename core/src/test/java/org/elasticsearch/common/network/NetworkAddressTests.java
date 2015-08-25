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

package org.elasticsearch.common.network;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Tests for network address formatting. Please avoid using any methods that cause DNS lookups!
 */
public class NetworkAddressTests extends ESTestCase {
    
    public void testFormatV4() throws Exception {
        assertEquals("localhost/127.0.0.1", NetworkAddress.format(forge("localhost", "127.0.0.1")));
        assertEquals("127.0.0.1", NetworkAddress.format(forge(null, "127.0.0.1")));
    }
    
    public void testFormatV6() throws Exception {
        assertEquals("localhost/::1", NetworkAddress.format(forge("localhost", "::1")));
        assertEquals("::1", NetworkAddress.format(forge(null, "::1")));
    }
    
    public void testFormatAddressV4() throws Exception {
        assertEquals("127.0.0.1", NetworkAddress.formatAddress(forge("localhost", "127.0.0.1")));
        assertEquals("127.0.0.1", NetworkAddress.formatAddress(forge(null, "127.0.0.1")));
    }
    
    public void testFormatAddressV6() throws Exception {
        assertEquals("::1", NetworkAddress.formatAddress(forge("localhost", "::1")));
        assertEquals("::1", NetworkAddress.formatAddress(forge(null, "::1")));
    }
    
    public void testFormatPortV4() throws Exception {
        assertEquals("localhost/127.0.0.1:1234", NetworkAddress.format(new InetSocketAddress(forge("localhost", "127.0.0.1"), 1234)));
        assertEquals("127.0.0.1:1234", NetworkAddress.format(new InetSocketAddress(forge(null, "127.0.0.1"), 1234)));
    }
    
    public void testFormatPortV6() throws Exception {
        assertEquals("localhost/[::1]:1234", NetworkAddress.format(new InetSocketAddress(forge("localhost", "::1"), 1234)));
        assertEquals("[::1]:1234",NetworkAddress.format(new InetSocketAddress(forge(null, "::1"), 1234)));
    }
    
    public void testFormatAddressPortV4() throws Exception {
        assertEquals("127.0.0.1:1234", NetworkAddress.formatAddress(new InetSocketAddress(forge("localhost", "127.0.0.1"), 1234)));
        assertEquals("127.0.0.1:1234", NetworkAddress.formatAddress(new InetSocketAddress(forge(null, "127.0.0.1"), 1234)));
    }
    
    public void testFormatAddressPortV6() throws Exception {
        assertEquals("[::1]:1234", NetworkAddress.formatAddress(new InetSocketAddress(forge("localhost", "::1"), 1234)));
        assertEquals("[::1]:1234", NetworkAddress.formatAddress(new InetSocketAddress(forge(null, "::1"), 1234)));
    }
    
    public void testNoScopeID() throws Exception {
        assertEquals("::1", NetworkAddress.format(forgeScoped(null, "::1", 5)));
        assertEquals("localhost/::1", NetworkAddress.format(forgeScoped("localhost", "::1", 5)));
        
        assertEquals("::1", NetworkAddress.formatAddress(forgeScoped(null, "::1", 5)));
        assertEquals("::1", NetworkAddress.formatAddress(forgeScoped("localhost", "::1", 5)));
        
        assertEquals("[::1]:1234", NetworkAddress.format(new InetSocketAddress(forgeScoped(null, "::1", 5), 1234)));
        assertEquals("localhost/[::1]:1234", NetworkAddress.format(new InetSocketAddress(forgeScoped("localhost", "::1", 5), 1234)));
        
        assertEquals("[::1]:1234", NetworkAddress.formatAddress(new InetSocketAddress(forgeScoped(null, "::1", 5), 1234)));
        assertEquals("[::1]:1234", NetworkAddress.formatAddress(new InetSocketAddress(forgeScoped("localhost", "::1", 5), 1234)));
    }
    
    /** creates address without any lookups. hostname can be null, for missing */
    private InetAddress forge(String hostname, String address) throws IOException {
        byte bytes[] = InetAddress.getByName(address).getAddress();
        return InetAddress.getByAddress(hostname, bytes);
    }
    
    /** creates scoped ipv6 address without any lookups. hostname can be null, for missing */
    private InetAddress forgeScoped(String hostname, String address, int scopeid) throws IOException {
        byte bytes[] = InetAddress.getByName(address).getAddress();
        return Inet6Address.getByAddress(hostname, bytes, scopeid);
    }
}
