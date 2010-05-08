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

package org.elasticsearch.monitor;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.monitor.jvm.JvmMonitorService;
import org.elasticsearch.monitor.jvm.JvmService;
import org.elasticsearch.monitor.memory.MemoryMonitorService;
import org.elasticsearch.monitor.network.NetworkService;
import org.elasticsearch.monitor.os.OsService;
import org.elasticsearch.monitor.process.ProcessService;
import org.elasticsearch.util.component.AbstractLifecycleComponent;
import org.elasticsearch.util.guice.inject.Inject;
import org.elasticsearch.util.settings.Settings;

/**
 * @author kimchy (shay.banon)
 */
public class MonitorService extends AbstractLifecycleComponent<MonitorService> {

    private final MemoryMonitorService memoryMonitorService;

    private final JvmMonitorService jvmMonitorService;

    private final OsService osService;

    private final ProcessService processService;

    private final JvmService jvmService;

    private final NetworkService networkService;

    @Inject public MonitorService(Settings settings, MemoryMonitorService memoryMonitorService, JvmMonitorService jvmMonitorService,
                                  OsService osService, ProcessService processService, JvmService jvmService, NetworkService networkService) {
        super(settings);
        this.memoryMonitorService = memoryMonitorService;
        this.jvmMonitorService = jvmMonitorService;
        this.osService = osService;
        this.processService = processService;
        this.jvmService = jvmService;
        this.networkService = networkService;
    }

    public OsService osService() {
        return this.osService;
    }

    public ProcessService processService() {
        return this.processService;
    }

    public JvmService jvmService() {
        return this.jvmService;
    }

    public NetworkService networkService() {
        return this.networkService;
    }

    @Override protected void doStart() throws ElasticSearchException {
        memoryMonitorService.start();
        jvmMonitorService.start();
    }

    @Override protected void doStop() throws ElasticSearchException {
        memoryMonitorService.stop();
        jvmMonitorService.stop();
    }

    @Override protected void doClose() throws ElasticSearchException {
        memoryMonitorService.close();
        jvmMonitorService.close();
    }
}
