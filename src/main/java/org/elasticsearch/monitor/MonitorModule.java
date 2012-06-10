/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.monitor;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Scopes;
import org.elasticsearch.common.inject.assistedinject.FactoryProvider;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.monitor.dump.DumpContributorFactory;
import org.elasticsearch.monitor.dump.DumpMonitorService;
import org.elasticsearch.monitor.dump.cluster.ClusterDumpContributor;
import org.elasticsearch.monitor.dump.heap.HeapDumpContributor;
import org.elasticsearch.monitor.dump.summary.SummaryDumpContributor;
import org.elasticsearch.monitor.dump.thread.ThreadDumpContributor;
import org.elasticsearch.monitor.fs.FsProbe;
import org.elasticsearch.monitor.fs.FsService;
import org.elasticsearch.monitor.fs.JmxFsProbe;
import org.elasticsearch.monitor.fs.SigarFsProbe;
import org.elasticsearch.monitor.jvm.JvmMonitorService;
import org.elasticsearch.monitor.jvm.JvmService;
import org.elasticsearch.monitor.network.JmxNetworkProbe;
import org.elasticsearch.monitor.network.NetworkProbe;
import org.elasticsearch.monitor.network.NetworkService;
import org.elasticsearch.monitor.network.SigarNetworkProbe;
import org.elasticsearch.monitor.os.JmxOsProbe;
import org.elasticsearch.monitor.os.OsProbe;
import org.elasticsearch.monitor.os.OsService;
import org.elasticsearch.monitor.os.SigarOsProbe;
import org.elasticsearch.monitor.process.JmxProcessProbe;
import org.elasticsearch.monitor.process.ProcessProbe;
import org.elasticsearch.monitor.process.ProcessService;
import org.elasticsearch.monitor.process.SigarProcessProbe;
import org.elasticsearch.monitor.sigar.SigarService;

import java.util.Map;

import static org.elasticsearch.monitor.dump.cluster.ClusterDumpContributor.CLUSTER;
import static org.elasticsearch.monitor.dump.heap.HeapDumpContributor.HEAP_DUMP;
import static org.elasticsearch.monitor.dump.summary.SummaryDumpContributor.SUMMARY;
import static org.elasticsearch.monitor.dump.thread.ThreadDumpContributor.THREAD_DUMP;

/**
 *
 */
public class MonitorModule extends AbstractModule {

    public static final class MonitorSettings {
        public static final String MEMORY_MANAGER_TYPE = "monitor.memory.type";
    }

    private final Settings settings;

    public MonitorModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    protected void configure() {
        boolean sigarLoaded = false;
        try {
            settings.getClassLoader().loadClass("org.hyperic.sigar.Sigar");
            SigarService sigarService = new SigarService(settings);
            if (sigarService.sigarAvailable()) {
                bind(SigarService.class).toInstance(sigarService);
                bind(ProcessProbe.class).to(SigarProcessProbe.class).asEagerSingleton();
                bind(OsProbe.class).to(SigarOsProbe.class).asEagerSingleton();
                bind(NetworkProbe.class).to(SigarNetworkProbe.class).asEagerSingleton();
                bind(FsProbe.class).to(SigarFsProbe.class).asEagerSingleton();
                sigarLoaded = true;
            }
        } catch (Throwable e) {
            // no sigar
            Loggers.getLogger(SigarService.class).trace("failed to load sigar", e);
        }
        if (!sigarLoaded) {
            // bind non sigar implementations
            bind(ProcessProbe.class).to(JmxProcessProbe.class).asEagerSingleton();
            bind(OsProbe.class).to(JmxOsProbe.class).asEagerSingleton();
            bind(NetworkProbe.class).to(JmxNetworkProbe.class).asEagerSingleton();
            bind(FsProbe.class).to(JmxFsProbe.class).asEagerSingleton();
        }
        // bind other services
        bind(ProcessService.class).asEagerSingleton();
        bind(OsService.class).asEagerSingleton();
        bind(NetworkService.class).asEagerSingleton();
        bind(JvmService.class).asEagerSingleton();
        bind(FsService.class).asEagerSingleton();

        bind(JvmMonitorService.class).asEagerSingleton();

        MapBinder<String, DumpContributorFactory> tokenFilterBinder
                = MapBinder.newMapBinder(binder(), String.class, DumpContributorFactory.class);

        Map<String, Settings> dumpContSettings = settings.getGroups("monitor.dump");
        for (Map.Entry<String, Settings> entry : dumpContSettings.entrySet()) {
            String dumpContributorName = entry.getKey();
            Settings dumpContributorSettings = entry.getValue();

            Class<? extends DumpContributorFactory> type = dumpContributorSettings.getAsClass("type", null, "org.elasticsearch.monitor.dump." + dumpContributorName + ".", "DumpContributor");
            if (type == null) {
                throw new IllegalArgumentException("Dump Contributor [" + dumpContributorName + "] must have a type associated with it");
            }
            tokenFilterBinder.addBinding(dumpContributorName).toProvider(FactoryProvider.newFactory(DumpContributorFactory.class, type)).in(Scopes.SINGLETON);
        }
        // add default
        if (!dumpContSettings.containsKey(SUMMARY)) {
            tokenFilterBinder.addBinding(SUMMARY).toProvider(FactoryProvider.newFactory(DumpContributorFactory.class, SummaryDumpContributor.class)).in(Scopes.SINGLETON);
        }
        if (!dumpContSettings.containsKey(THREAD_DUMP)) {
            tokenFilterBinder.addBinding(THREAD_DUMP).toProvider(FactoryProvider.newFactory(DumpContributorFactory.class, ThreadDumpContributor.class)).in(Scopes.SINGLETON);
        }
        if (!dumpContSettings.containsKey(HEAP_DUMP)) {
            tokenFilterBinder.addBinding(HEAP_DUMP).toProvider(FactoryProvider.newFactory(DumpContributorFactory.class, HeapDumpContributor.class)).in(Scopes.SINGLETON);
        }
        if (!dumpContSettings.containsKey(CLUSTER)) {
            tokenFilterBinder.addBinding(CLUSTER).toProvider(FactoryProvider.newFactory(DumpContributorFactory.class, ClusterDumpContributor.class)).in(Scopes.SINGLETON);
        }


        bind(DumpMonitorService.class).asEagerSingleton();
    }
}
