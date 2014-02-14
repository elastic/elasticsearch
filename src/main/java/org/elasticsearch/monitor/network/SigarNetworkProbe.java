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
import org.elasticsearch.monitor.sigar.SigarService;
import org.hyperic.sigar.*;

/**
 *
 */
public class SigarNetworkProbe extends AbstractComponent implements NetworkProbe {

    private final SigarService sigarService;

    @Inject
    public SigarNetworkProbe(Settings settings, SigarService sigarService) {
        super(settings);
        this.sigarService = sigarService;
    }

    @Override
    public NetworkInfo networkInfo() {
        Sigar sigar = sigarService.sigar();

        NetworkInfo networkInfo = new NetworkInfo();

        try {
            NetInterfaceConfig netInterfaceConfig = sigar.getNetInterfaceConfig(null);
            networkInfo.primary = new NetworkInfo.Interface(netInterfaceConfig.getName(), netInterfaceConfig.getAddress(), netInterfaceConfig.getHwaddr());
        } catch (SigarException e) {
            // ignore
        }

        return networkInfo;
    }

    @Override
    public synchronized NetworkStats networkStats() {
        Sigar sigar = sigarService.sigar();

        NetworkStats stats = new NetworkStats();
        stats.timestamp = System.currentTimeMillis();

        try {
            Tcp tcp = sigar.getTcp();
            stats.tcp = new NetworkStats.Tcp();
            stats.tcp.activeOpens = tcp.getActiveOpens();
            stats.tcp.passiveOpens = tcp.getPassiveOpens();
            stats.tcp.attemptFails = tcp.getAttemptFails();
            stats.tcp.estabResets = tcp.getEstabResets();
            stats.tcp.currEstab = tcp.getCurrEstab();
            stats.tcp.inSegs = tcp.getInSegs();
            stats.tcp.outSegs = tcp.getOutSegs();
            stats.tcp.retransSegs = tcp.getRetransSegs();
            stats.tcp.inErrs = tcp.getInErrs();
            stats.tcp.outRsts = tcp.getOutRsts();
        } catch (SigarException e) {
            // ignore
        }

        return stats;
    }

    @Override
    public String ifconfig() {
        Sigar sigar = sigarService.sigar();
        StringBuilder sb = new StringBuilder();
        try {
            for (String ifname : sigar.getNetInterfaceList()) {
                NetInterfaceConfig ifconfig = null;
                try {
                    ifconfig = sigar.getNetInterfaceConfig(ifname);
                } catch (SigarException e) {
                    sb.append(ifname + "\t" + "Not Avaialbe [" + e.getMessage() + "]");
                    continue;
                }
                long flags = ifconfig.getFlags();

                String hwaddr = "";
                if (!NetFlags.NULL_HWADDR.equals(ifconfig.getHwaddr())) {
                    hwaddr = " HWaddr " + ifconfig.getHwaddr();
                }

                if (!ifconfig.getName().equals(ifconfig.getDescription())) {
                    sb.append(ifconfig.getDescription()).append('\n');
                }

                sb.append(ifconfig.getName() + "\t" + "Link encap:" + ifconfig.getType() + hwaddr).append('\n');

                String ptp = "";
                if ((flags & NetFlags.IFF_POINTOPOINT) > 0) {
                    ptp = "  P-t-P:" + ifconfig.getDestination();
                }

                String bcast = "";
                if ((flags & NetFlags.IFF_BROADCAST) > 0) {
                    bcast = "  Bcast:" + ifconfig.getBroadcast();
                }

                sb.append("\t" +
                        "inet addr:" + ifconfig.getAddress() +
                        ptp + //unlikely
                        bcast +
                        "  Mask:" + ifconfig.getNetmask()).append('\n');

                sb.append("\t" +
                        NetFlags.getIfFlagsString(flags) +
                        " MTU:" + ifconfig.getMtu() +
                        "  Metric:" + ifconfig.getMetric()).append('\n');
                try {
                    NetInterfaceStat ifstat = sigar.getNetInterfaceStat(ifname);

                    sb.append("\t" +
                            "RX packets:" + ifstat.getRxPackets() +
                            " errors:" + ifstat.getRxErrors() +
                            " dropped:" + ifstat.getRxDropped() +
                            " overruns:" + ifstat.getRxOverruns() +
                            " frame:" + ifstat.getRxFrame()).append('\n');

                    sb.append("\t" +
                            "TX packets:" + ifstat.getTxPackets() +
                            " errors:" + ifstat.getTxErrors() +
                            " dropped:" + ifstat.getTxDropped() +
                            " overruns:" + ifstat.getTxOverruns() +
                            " carrier:" + ifstat.getTxCarrier()).append('\n');
                    sb.append("\t" + "collisions:" +
                            ifstat.getTxCollisions()).append('\n');

                    long rxBytes = ifstat.getRxBytes();
                    long txBytes = ifstat.getTxBytes();

                    sb.append("\t" +
                            "RX bytes:" + rxBytes +
                            " (" + Sigar.formatSize(rxBytes) + ")" +
                            "  " +
                            "TX bytes:" + txBytes +
                            " (" + Sigar.formatSize(txBytes) + ")").append('\n');
                } catch (SigarException e) {
                }
            }
            return sb.toString();
        } catch (SigarException e) {
            return "NA";
        }
    }
}
