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

package org.elasticsearch.monitor.sigar;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.hyperic.sigar.Sigar;

/**
 *
 */
public class SigarService extends AbstractComponent {

    private final Sigar sigar;

    @Inject
    public SigarService(Settings settings) {
        super(settings);

        Sigar sigar = null;
        try {
            sigar = new Sigar();
            // call it to make sure the library was loaded
            sigar.getPid();
            logger.trace("sigar loaded successfully");
        } catch (Throwable t) {
            logger.trace("failed to load sigar", t);
            if (sigar != null) {
                try {
                    sigar.close();
                } catch (Throwable t1) {
                    // ignore
                } finally {
                    sigar = null;
                }
            }
        }
        this.sigar = sigar;
    }

    public boolean sigarAvailable() {
        return sigar != null;
    }

    public Sigar sigar() {
        return this.sigar;
    }
}
