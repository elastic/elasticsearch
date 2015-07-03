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

package org.elasticsearch.plugin.probe.sigar;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.PreProcessModule;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.monitor.process.ProcessModule;
import org.elasticsearch.monitor.process.SigarProcessProbe;

public class SigarModule extends AbstractModule implements PreProcessModule {

    private final ESLogger logger;
    private final SigarService sigarService;

    public SigarModule(Settings settings) {
        this.logger = Loggers.getLogger(getClass(), settings);
        this.sigarService = loadSigar(settings);
    }

    private SigarService loadSigar(Settings settings) {
        try {
            settings.getClassLoader().loadClass("org.hyperic.sigar.Sigar");
            SigarService sigarService = new SigarService(settings);
            if (sigarService.sigarAvailable()) {
                return sigarService;
            } else {
                logger.warn("sigar is not available");
            }
        } catch (Throwable e) {
            logger.warn("failed to load sigar", e);
        }
        return null;
    }

    @Override
    protected void configure() {
        if (sigarService != null) {
            bind(SigarService.class).toInstance(sigarService);
        }
    }

    @Override
    public void processModule(Module module) {
        if (sigarService == null) {
            return;
        }

        if (module instanceof ProcessModule) {
            ProcessModule processModule = (ProcessModule) module;
            processModule.registerProbe(SigarProcessProbe.TYPE, SigarProcessProbe.class);
        }
    }
}
