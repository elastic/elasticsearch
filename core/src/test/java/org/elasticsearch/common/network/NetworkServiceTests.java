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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.net.InetAddress;

/**
 * Tests for network service... try to keep them safe depending upon configuration
 * please don't actually bind to anything, just test the addresses.
 */
public class NetworkServiceTests extends ESTestCase {

    /** 
     * ensure exception if we bind to multicast ipv4 address 
     */
    public void testBindMulticastV4() throws Exception {
        NetworkService service = new NetworkService(Settings.EMPTY);
        try {
            service.resolveBindHostAddress("239.1.1.1");
            fail("should have hit exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("invalid: multicast"));
        }
    }
    
    /** 
     * ensure exception if we bind to multicast ipv6 address 
     */
    public void testBindMulticastV6() throws Exception {
        NetworkService service = new NetworkService(Settings.EMPTY);
        try {
            service.resolveBindHostAddress("FF08::108");
            fail("should have hit exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("invalid: multicast"));
        }
    }
    
    /** 
     * ensure exception if we publish to multicast ipv4 address 
     */
    public void testPublishMulticastV4() throws Exception {
        NetworkService service = new NetworkService(Settings.EMPTY);
        try {
            service.resolvePublishHostAddress("239.1.1.1");
            fail("should have hit exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("invalid: multicast"));
        }
    }
    
    /** 
     * ensure exception if we publish to multicast ipv6 address 
     */
    public void testPublishMulticastV6() throws Exception {
        NetworkService service = new NetworkService(Settings.EMPTY);
        try {
            service.resolvePublishHostAddress("FF08::108");
            fail("should have hit exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("invalid: multicast"));
        }
    }

    /** 
     * ensure specifying wildcard ipv4 address will bind to all interfaces 
     */
    public void testBindAnyLocalV4() throws Exception {
        NetworkService service = new NetworkService(Settings.EMPTY);
        assertEquals(InetAddress.getByName("0.0.0.0"), service.resolveBindHostAddress("0.0.0.0")[0]);
    }
    
    /** 
     * ensure specifying wildcard ipv6 address will bind to all interfaces 
     */
    public void testBindAnyLocalV6() throws Exception {
        NetworkService service = new NetworkService(Settings.EMPTY);
        assertEquals(InetAddress.getByName("::"), service.resolveBindHostAddress("::")[0]);
    }

    /** 
     * ensure specifying wildcard ipv4 address selects reasonable publish address 
     */
    public void testPublishAnyLocalV4() throws Exception {
        InetAddress expected = null;
        try {
            expected = NetworkUtils.getFirstNonLoopbackAddresses()[0];
        } catch (Exception e) {
            assumeNoException("test requires up-and-running non-loopback address", e);
        }
        
        NetworkService service = new NetworkService(Settings.EMPTY);
        assertEquals(expected, service.resolvePublishHostAddress("0.0.0.0"));
    }

    /** 
     * ensure specifying wildcard ipv6 address selects reasonable publish address 
     */
    public void testPublishAnyLocalV6() throws Exception {
        InetAddress expected = null;
        try {
            expected = NetworkUtils.getFirstNonLoopbackAddresses()[0];
        } catch (Exception e) {
            assumeNoException("test requires up-and-running non-loopback address", e);
        }
        
        NetworkService service = new NetworkService(Settings.EMPTY);
        assertEquals(expected, service.resolvePublishHostAddress("::"));
    }
}
