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

package org.elasticsearch.monitor.network;

import org.elasticsearch.monitor.sigar.SigarService;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.guice.inject.Inject;
import org.elasticsearch.util.settings.Settings;
import org.hyperic.sigar.NetInterfaceConfig;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;
import org.hyperic.sigar.Tcp;

/**
 * @author kimchy (shay.banon)
 */
public class SigarNetworkProbe extends AbstractComponent implements NetworkProbe {

    private final SigarService sigarService;

    @Inject public SigarNetworkProbe(Settings settings, SigarService sigarService) {
        super(settings);
        this.sigarService = sigarService;
    }

    @Override public NetworkInfo networkInfo() {
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

    @Override public synchronized NetworkStats networkStats() {
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
}
