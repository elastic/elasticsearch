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

package org.elasticsearch.util.network;

import org.elasticsearch.util.MapBuilder;
import org.elasticsearch.util.collect.ImmutableMap;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.guice.inject.Inject;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Collection;

/**
 * @author kimchy (shay.banon)
 */
public class NetworkService extends AbstractComponent {

    public static final String LOCAL = "#local#";

    public static final String GLOBAL_NETWORK_BINDHOST_SETTING = "network.bind_host";
    public static final String GLOBAL_NETWORK_PUBLISHHOST_SETTING = "network.publish_host";

    public static interface CustomNameResolver {
        InetAddress resolve();
    }

    private volatile ImmutableMap<String, CustomNameResolver> customNameResolvers = ImmutableMap.of();

    @Inject public NetworkService(Settings settings) {
        super(settings);
    }

    public void addCustomNameResolver(String name, CustomNameResolver customNameResolver) {
        if (!(name.startsWith("#") && name.endsWith("#"))) {
            name = "#" + name + "#";
        }
        customNameResolvers = MapBuilder.<String, CustomNameResolver>newMapBuilder().putAll(customNameResolvers).put(name, customNameResolver).immutableMap();
    }


    public InetAddress resolveBindHostAddress(String bindHost) throws IOException {
        return resolveBindHostAddress(bindHost, null);
    }

    public InetAddress resolveBindHostAddress(String bindHost, String defaultValue2) throws IOException {
        return resolveInetAddress(bindHost, settings.get(GLOBAL_NETWORK_BINDHOST_SETTING), defaultValue2);
    }

    public InetAddress resolvePublishHostAddress(String publishHost) throws IOException {
        InetAddress address = resolvePublishHostAddress(publishHost, null);
        // verify that its not a local address
        if (address == null || address.isAnyLocalAddress()) {
            address = NetworkUtils.getLocalAddress();
        }
        return address;
    }

    public InetAddress resolvePublishHostAddress(String publishHost, String defaultValue2) throws IOException {
        return resolveInetAddress(publishHost, settings.get(GLOBAL_NETWORK_PUBLISHHOST_SETTING), defaultValue2);
    }

    public InetAddress resolveInetAddress(String host, String defaultValue1, String defaultValue2) throws UnknownHostException, IOException {
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

            CustomNameResolver customNameResolver = customNameResolvers.get(host);
            if (customNameResolver != null) {
                return customNameResolver.resolve();
            }

            if (host.equals("local")) {
                return NetworkUtils.getLocalAddress();
            } else {
                Collection<NetworkInterface> allInterfs = NetworkUtils.getAllAvailableInterfaces();
                for (NetworkInterface ni : allInterfs) {
                    if (!ni.isUp() || ni.isLoopback()) {
                        continue;
                    }
                    if (host.equals(ni.getName()) || host.equals(ni.getDisplayName())) {
                        return NetworkUtils.getFirstNonLoopbackAddress(ni, NetworkUtils.getIpStackType());
                    }
                }
            }
            throw new IOException("Failed to find network interface for [" + host + "]");
        }
        return InetAddress.getByName(host);
    }
}
