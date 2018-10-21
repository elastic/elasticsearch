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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Constants;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Utilities for network interfaces / addresses binding and publishing.
 * Its only intended for that purpose, not general purpose usage!!!!
 */
public abstract class NetworkUtils {

    /** no instantiation */
    private NetworkUtils() {}
    
    /**
     * By default we bind to any addresses on an interface/name, unless restricted by :ipv4 etc.
     * This property is unrelated to that, this is about what we *publish*. Today the code pretty much
     * expects one address so this is used for the sort order.
     * @deprecated transition mechanism only
     */
    @Deprecated
    static final boolean PREFER_V6 = Boolean.parseBoolean(System.getProperty("java.net.preferIPv6Addresses", "false"));

    /**
     * True if we can bind to a v6 address. Its silly, but for *binding* we have a need to know
     * if the stack works. this can prevent scary noise on IPv4-only hosts.
     * @deprecated transition mechanism only, do not use
     */
    @Deprecated
    public static final boolean SUPPORTS_V6;

    static {
        boolean v = false;
        try {
            for (NetworkInterface nic : getInterfaces()) {
                for (InetAddress address : Collections.list(nic.getInetAddresses())) {
                    if (address instanceof Inet6Address) {
                        v = true;
                        break;
                    }
                }
            }
        } catch (SecurityException | SocketException misconfiguration) {
            v = true; // be optimistic, you misconfigure, then you get noise to your screen
        }
        SUPPORTS_V6 = v;
    }
    
    /** Sorts an address by preference. This way code like publishing can just pick the first one */
    static int sortKey(InetAddress address, boolean prefer_v6) {
        int key = address.getAddress().length;
        if (prefer_v6) {
            key = -key;
        }
        
        if (address.isAnyLocalAddress()) {
            key += 5;
        }
        if (address.isMulticastAddress()) {
            key += 4;
        }
        if (address.isLoopbackAddress()) {
            key += 3;
        }
        if (address.isLinkLocalAddress()) {
            key += 2;
        }
        if (address.isSiteLocalAddress()) {
            key += 1;
        }

        return key;
    }

    /** 
     * Sorts addresses by order of preference. This is used to pick the first one for publishing
     * @deprecated remove this when multihoming is really correct
     */
    @Deprecated
    // only public because of silly multicast
    public static void sortAddresses(List<InetAddress> list) {
        Collections.sort(list, new Comparator<InetAddress>() {
            @Override
            public int compare(InetAddress left, InetAddress right) {
                int cmp = Integer.compare(sortKey(left, PREFER_V6), sortKey(right, PREFER_V6));
                if (cmp == 0) {
                    cmp = new BytesRef(left.getAddress()).compareTo(new BytesRef(right.getAddress()));
                }
                return cmp;
            }
        });
    }
    
    /** Return all interfaces (and subinterfaces) on the system */
    static List<NetworkInterface> getInterfaces() throws SocketException {
        List<NetworkInterface> all = new ArrayList<>();
        addAllInterfaces(all, Collections.list(NetworkInterface.getNetworkInterfaces()));
        Collections.sort(all, new Comparator<NetworkInterface>() {
            @Override
            public int compare(NetworkInterface left, NetworkInterface right) {
                return Integer.compare(left.getIndex(), right.getIndex());
            }
        });
        return all;
    }
    
    /** Helper for getInterfaces, recursively adds subinterfaces to {@code target} */
    private static void addAllInterfaces(List<NetworkInterface> target, List<NetworkInterface> level) {
        if (!level.isEmpty()) {
            target.addAll(level);
            for (NetworkInterface intf : level) {
                addAllInterfaces(target, Collections.list(intf.getSubInterfaces()));
            }
        }
    }
    
    /** Returns system default for SO_REUSEADDR */
    public static boolean defaultReuseAddress() {
        return Constants.WINDOWS ? false : true;
    }

    /** Returns all interface-local scope (loopback) addresses for interfaces that are up. */
    static InetAddress[] getLoopbackAddresses() throws SocketException {
        List<InetAddress> list = new ArrayList<>();
        for (NetworkInterface intf : getInterfaces()) {
            if (intf.isUp()) {
                for (InetAddress address : Collections.list(intf.getInetAddresses())) {
                    if (address.isLoopbackAddress()) {
                        list.add(address);
                    }
                }
            }
        }
        if (list.isEmpty()) {
            throw new IllegalArgumentException("No up-and-running loopback addresses found, got " + getInterfaces());
        }
        return list.toArray(new InetAddress[list.size()]);
    }
    
    /** Returns all site-local scope (private) addresses for interfaces that are up. */
    static InetAddress[] getSiteLocalAddresses() throws SocketException {
        List<InetAddress> list = new ArrayList<>();
        for (NetworkInterface intf : getInterfaces()) {
            if (intf.isUp()) {
                for (InetAddress address : Collections.list(intf.getInetAddresses())) {
                    if (address.isSiteLocalAddress()) {
                        list.add(address);
                    }
                }
            }
        }
        if (list.isEmpty()) {
            throw new IllegalArgumentException("No up-and-running site-local (private) addresses found, got " + getInterfaces());
        }
        return list.toArray(new InetAddress[list.size()]);
    }
    
    /** Returns all global scope addresses for interfaces that are up. */
    static InetAddress[] getGlobalAddresses() throws SocketException {
        List<InetAddress> list = new ArrayList<>();
        for (NetworkInterface intf : getInterfaces()) {
            if (intf.isUp()) {
                for (InetAddress address : Collections.list(intf.getInetAddresses())) {
                    if (address.isLoopbackAddress() == false && 
                        address.isSiteLocalAddress() == false &&
                        address.isLinkLocalAddress() == false) {
                        list.add(address);
                    }
                }
            }
        }
        if (list.isEmpty()) {
            throw new IllegalArgumentException("No up-and-running global-scope (public) addresses found, got " + getInterfaces());
        }
        return list.toArray(new InetAddress[list.size()]);
    }
    
    /** Returns all addresses (any scope) for interfaces that are up. 
     *  This is only used to pick a publish address, when the user set network.host to a wildcard */
    static InetAddress[] getAllAddresses() throws SocketException {
        List<InetAddress> list = new ArrayList<>();
        for (NetworkInterface intf : getInterfaces()) {
            if (intf.isUp()) {
                for (InetAddress address : Collections.list(intf.getInetAddresses())) {
                    list.add(address);
                }
            }
        }
        if (list.isEmpty()) {
            throw new IllegalArgumentException("No up-and-running addresses found, got " + getInterfaces());
        }
        return list.toArray(new InetAddress[list.size()]);
    }
    
    /** Returns addresses for the given interface (it must be marked up) */
    static InetAddress[] getAddressesForInterface(String name) throws SocketException {
        Optional<NetworkInterface> networkInterface = getInterfaces().stream().filter((netIf) -> name.equals(netIf.getName())).findFirst();

        if (networkInterface.isPresent() == false) {
            throw new IllegalArgumentException("No interface named '" + name + "' found, got " + getInterfaces());
        }
        if (!networkInterface.get().isUp()) {
            throw new IllegalArgumentException("Interface '" + name + "' is not up and running");
        }
        List<InetAddress> list = Collections.list(networkInterface.get().getInetAddresses());
        if (list.isEmpty()) {
            throw new IllegalArgumentException("Interface '" + name + "' has no internet addresses");
        }
        return list.toArray(new InetAddress[list.size()]);
    }
    
    /** Returns only the IPV4 addresses in {@code addresses} */
    static InetAddress[] filterIPV4(InetAddress addresses[]) {
        List<InetAddress> list = new ArrayList<>();
        for (InetAddress address : addresses) {
            if (address instanceof Inet4Address) {
                list.add(address);
            }
        }
        if (list.isEmpty()) {
            throw new IllegalArgumentException("No ipv4 addresses found in " + Arrays.toString(addresses));
        }
        return list.toArray(new InetAddress[list.size()]);
    }
    
    /** Returns only the IPV6 addresses in {@code addresses} */
    static InetAddress[] filterIPV6(InetAddress addresses[]) {
        List<InetAddress> list = new ArrayList<>();
        for (InetAddress address : addresses) {
            if (address instanceof Inet6Address) {
                list.add(address);
            }
        }
        if (list.isEmpty()) {
            throw new IllegalArgumentException("No ipv6 addresses found in " + Arrays.toString(addresses));
        }
        return list.toArray(new InetAddress[list.size()]);
    }
}
