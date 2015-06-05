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

package org.elasticsearch.monitor.network;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.SingleObjectCache;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

/**
 *
 */
public final class NetworkService extends AbstractComponent {

    private final NetworkProbe probe;

    private final NetworkInfo info;

    private final SingleObjectCache<NetworkStats> networkStatsCache;

    @Inject
    public NetworkService(Settings settings, NetworkProbe probe) {
        super(settings);
        this.probe = probe;

        TimeValue refreshInterval = settings.getAsTime("monitor.network.refresh_interval", TimeValue.timeValueSeconds(5));

        logger.debug("Using probe [{}] with refresh_interval [{}]", probe, refreshInterval);

        this.info = probe.networkInfo();
        this.info.refreshInterval = refreshInterval.millis();
        networkStatsCache = new NetworkStatsCache(refreshInterval, probe.networkStats());
        if (logger.isDebugEnabled()) {
            StringBuilder netDebug = new StringBuilder("net_info");
            try {
                Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
                String hostName = InetAddress.getLocalHost().getHostName();
                netDebug.append("\nhost [").append(hostName).append("]\n");
                while (interfaces.hasMoreElements()) {
                    NetworkInterface net = interfaces.nextElement();

                    netDebug.append(net.getName()).append('\t').append("display_name [").append(net.getDisplayName()).append("]\n");
                    Enumeration<InetAddress> addresses = net.getInetAddresses();
                    netDebug.append("\t\taddress ");
                    while (addresses.hasMoreElements()) {
                        netDebug.append("[").append(addresses.nextElement()).append("] ");
                    }
                    netDebug.append('\n');
                    netDebug.append("\t\tmtu [").append(net.getMTU()).append("] multicast [").append(net.supportsMulticast()).append("] ptp [").append(net.isPointToPoint())
                            .append("] loopback [").append(net.isLoopback()).append("] up [").append(net.isUp()).append("] virtual [").append(net.isVirtual()).append("]")
                            .append('\n');

                    Enumeration<NetworkInterface> subInterfaces = net.getSubInterfaces();
                    if (subInterfaces != null && subInterfaces.hasMoreElements()) {
                        netDebug.append("\t\t\tsub interfaces:\n");

                        while (subInterfaces.hasMoreElements()) {

                            net = subInterfaces.nextElement();

                            netDebug.append("\t\t\t").append(net.getName()).append("\t").append("display_name [").append(net.getDisplayName()).append("]\n");
                            addresses = net.getInetAddresses();
                            netDebug.append("\t\t\t\t\taddress ");
                            while (addresses.hasMoreElements()) {
                                netDebug.append("[").append(addresses.nextElement()).append("] ");
                            }
                            netDebug.append('\n');
                            netDebug.append("\t\t\t\t\tmtu [").append(net.getMTU()).append("] multicast [").append(net.supportsMulticast()).append("] ptp [").append(net.isPointToPoint())
                                    .append("] loopback [").append(net.isLoopback()).append("] up [").append(net.isUp()).append("] virtual [").append(net.isVirtual()).append("]")
                                    .append('\n');
                        }
                    }
                }
            } catch (Exception ex) {
                netDebug.append("failed to get Network Interface Info [" + ex.getMessage() + "]");
            }
            logger.debug(netDebug.toString());
        }

        if (logger.isTraceEnabled()) {
            logger.trace("ifconfig\n\n" + ifconfig());
        }
        stats(); // pull the stats one time
    }

    public NetworkInfo info() {
        return this.info;
    }

    public  NetworkStats stats() {
        return networkStatsCache.getOrRefresh();
    }

    private class NetworkStatsCache extends SingleObjectCache<NetworkStats> {
        public NetworkStatsCache(TimeValue interval, NetworkStats initValue) {
            super(interval, initValue);
        }

        @Override
        protected NetworkStats refresh() {
            return probe.networkStats();
        }
    }

    public String ifconfig() {
        return probe.ifconfig();
    }
}
