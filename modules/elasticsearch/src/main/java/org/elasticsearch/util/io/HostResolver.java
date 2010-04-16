/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.util.io;

import org.elasticsearch.util.settings.Settings;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;

/**
 * @author kimchy (Shay Banon)
 */
public abstract class HostResolver {

    public static final String GLOBAL_NETWORK_BINDHOST_SETTING = "network.bind_host";
    public static final String GLOBAL_NETWORK_PUBLISHHOST_SETTING = "network.publish_host";

    public static final String LOCAL_IP = "#local:ip#";
    public static final String LOCAL_HOST = "#local:host#";
    public static final String LOCAL_CANONICALHOST = "#local:canonicalhost#";

    public static boolean isIPv4() {
        return System.getProperty("java.net.preferIPv4Stack") != null && System.getProperty("java.net.preferIPv4Stack").equals("true");
    }

    public static InetAddress resolveBindHostAddress(String bindHost, Settings settings) throws IOException {
        return resolveBindHostAddress(bindHost, settings, null);
    }

    public static InetAddress resolveBindHostAddress(String bindHost, Settings settings, String defaultValue2) throws IOException {
        return resolveInetAddress(bindHost, settings.get(GLOBAL_NETWORK_BINDHOST_SETTING), defaultValue2);
    }

    public static InetAddress resolvePublishHostAddress(String publishHost, Settings settings) throws IOException {
        return resolvePublishHostAddress(publishHost, settings, null);
    }

    public static InetAddress resolvePublishHostAddress(String publishHost, Settings settings, String defaultValue2) throws IOException {
        return resolveInetAddress(publishHost, settings.get(GLOBAL_NETWORK_PUBLISHHOST_SETTING), defaultValue2);
    }

    public static InetAddress resolveInetAddress(String host, String defaultValue1, String defaultValue2) throws UnknownHostException, IOException {
        String resolvedHost = resolveHost(host, defaultValue1, defaultValue2);
        if (resolvedHost == null) {
            return null;
        }
        return InetAddress.getByName(resolvedHost);
    }

    public static String resolveHost(String host, String defaultValue1, String defaultValue2) throws UnknownHostException, IOException {
        if (host == null) {
            host = defaultValue1;
        }
        if (host == null) {
            host = defaultValue2;
        }
        if (host == null) {
            return null;
        }
        if (host.startsWith("#") && host.endsWith("#")) {
            host = host.substring(1, host.length() - 1);
            if (host.equals("local:ip")) {
                return InetAddress.getLocalHost().getHostAddress();
            } else if (host.equalsIgnoreCase("local:host")) {
                return InetAddress.getLocalHost().getHostName();
            } else if (host.equalsIgnoreCase("local:canonicalhost")) {
                return InetAddress.getLocalHost().getCanonicalHostName();
            } else {
                String name = host.substring(0, host.indexOf(':'));
                String type = host.substring(host.indexOf(':') + 1);
                Enumeration<NetworkInterface> niEnum;
                try {
                    niEnum = NetworkInterface.getNetworkInterfaces();
                } catch (SocketException e) {
                    throw new IOException("Failed to get network interfaces", e);
                }
                while (niEnum.hasMoreElements()) {
                    NetworkInterface ni = niEnum.nextElement();
                    if (name.equals(ni.getName()) || name.equals(ni.getDisplayName())) {
                        Enumeration<InetAddress> inetEnum = ni.getInetAddresses();
                        while (inetEnum.hasMoreElements()) {
                            InetAddress addr = inetEnum.nextElement();
                            if (addr.getHostAddress().equals("127.0.0.1")) {
                                // ignore local host
                                continue;
                            }
                            if (addr.getHostAddress().indexOf(".") == -1) {
                                // ignore address like 0:0:0:0:0:0:0:1
                                continue;
                            }
                            if ("host".equalsIgnoreCase(type)) {
                                return addr.getHostName();
                            } else if ("canonicalhost".equalsIgnoreCase(type)) {
                                return addr.getCanonicalHostName();
                            } else {
                                return addr.getHostAddress();
                            }
                        }
                    }
                }
            }
            throw new IOException("Failed to find network interface for [" + host + "]");
        }
        InetAddress inetAddress = java.net.InetAddress.getByName(host);
        String hostAddress = inetAddress.getHostAddress();
        String hostName = inetAddress.getHostName();
        String canonicalHostName = inetAddress.getCanonicalHostName();
        if (host.equalsIgnoreCase(hostAddress)) {
            return hostAddress;
        } else if (host.equalsIgnoreCase(canonicalHostName)) {
            return canonicalHostName;
        } else {
            return hostName; //resolve property into actual lower/upper case
        }
    }

    private HostResolver() {

    }
}
