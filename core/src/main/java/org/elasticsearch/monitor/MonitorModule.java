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

package org.elasticsearch.monitor;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.monitor.fs.FsProbe;
import org.elasticsearch.monitor.fs.FsService;
import org.elasticsearch.monitor.jvm.JvmMonitorService;
import org.elasticsearch.monitor.jvm.JvmService;
import org.elasticsearch.monitor.os.OsProbe;
import org.elasticsearch.monitor.os.OsService;
import org.elasticsearch.monitor.process.ProcessProbe;
import org.elasticsearch.monitor.process.ProcessService;

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
        // bind default implementations
        bind(ProcessProbe.class).toInstance(ProcessProbe.getInstance());
        bind(OsProbe.class).toInstance(OsProbe.getInstance());
        bind(FsProbe.class).asEagerSingleton();

        // bind other services
        bind(ProcessService.class).asEagerSingleton();
        bind(OsService.class).asEagerSingleton();
        bind(JvmService.class).asEagerSingleton();
        bind(FsService.class).asEagerSingleton();

        bind(JvmMonitorService.class).asEagerSingleton();
    }
}
