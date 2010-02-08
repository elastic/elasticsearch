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

package org.elasticsearch.monitor.memory;

import com.google.inject.Inject;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.component.Lifecycle;
import org.elasticsearch.util.component.LifecycleComponent;
import org.elasticsearch.util.settings.Settings;

/**
 * @author kimchy (Shay Banon)
 */
public class MemoryMonitorService extends AbstractComponent implements LifecycleComponent<MemoryMonitorService> {

    private final Lifecycle lifecycle = new Lifecycle();

    private final MemoryMonitor memoryMonitor;

    @Inject public MemoryMonitorService(Settings settings, MemoryMonitor memoryMonitor) {
        super(settings);
        this.memoryMonitor = memoryMonitor;
    }

    @Override public Lifecycle.State lifecycleState() {
        return lifecycle.state();
    }

    @Override public MemoryMonitorService start() throws ElasticSearchException {
        if (!lifecycle.moveToStarted()) {
            return this;
        }
        memoryMonitor.start();
        return this;
    }

    @Override public MemoryMonitorService stop() throws ElasticSearchException {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        memoryMonitor.stop();
        return this;
    }

    @Override public void close() throws ElasticSearchException {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.moveToClosed()) {
            return;
        }
    }
}
