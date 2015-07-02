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

package org.elasticsearch.monitor.process;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.monitor.probe.CompositeProbe;
import org.elasticsearch.monitor.probe.ProbeRegistry;

import java.util.Collection;
import java.util.Map;

public class ProcessProbeRegistry extends ProbeRegistry<ProcessProbe> {

    @Inject
    public ProcessProbeRegistry(Map<String, ProcessProbe> probes) {
        super(probes);
    }

    @Override
    public ProcessProbe probe() {
        return new CompositeProcessProbe(probes());
    }

    class CompositeProcessProbe extends CompositeProbe<ProcessProbe> implements ProcessProbe {

        public CompositeProcessProbe(Collection<ProcessProbe> probes, Strategy strategy) {
            super(probes, strategy);
        }

        public CompositeProcessProbe(Collection<ProcessProbe> probes) {
            super(probes);
        }

        @Override
        public Long pid() {
            return executeOnProbes(new Callback<Long>() {
                @Override
                protected Long executeProbeOperation(ProcessProbe probe) throws UnsupportedOperationException {
                    return probe.pid();
                }
            });
        }

        @Override
        public Long maxFileDescriptor() {
            return executeOnProbes(new Callback<Long>() {
                @Override
                protected Long executeProbeOperation(ProcessProbe probe) throws UnsupportedOperationException {
                    return probe.maxFileDescriptor();
                }
            });
        }

        @Override
        public Long openFileDescriptor() {
            return executeOnProbes(new Callback<Long>() {
                @Override
                protected Long executeProbeOperation(ProcessProbe probe) throws UnsupportedOperationException {
                    return probe.openFileDescriptor();
                }
            });
        }

        @Override
        public Short processCpuLoad() {
            return executeOnProbes(new Callback<Short>() {
                @Override
                protected Short executeProbeOperation(ProcessProbe probe) throws UnsupportedOperationException {
                    return probe.processCpuLoad();
                }
            });
        }

        @Override
        public Long processCpuTime() {
            return executeOnProbes(new Callback<Long>() {
                @Override
                protected Long executeProbeOperation(ProcessProbe probe) throws UnsupportedOperationException {
                    return probe.processCpuTime();
                }
            });
        }

        @Override
        public Long totalVirtualMemorySize() {
            return executeOnProbes(new Callback<Long>() {
                @Override
                protected Long executeProbeOperation(ProcessProbe probe) throws UnsupportedOperationException {
                    return probe.totalVirtualMemorySize();
                }
            });
        }

        @Override
        public Long processSystemTime() {
            return executeOnProbes(new Callback<Long>() {
                @Override
                protected Long executeProbeOperation(ProcessProbe probe) throws UnsupportedOperationException {
                    return probe.processSystemTime();
                }
            });
        }

        @Override
        public Long processUserTime() {
            return executeOnProbes(new Callback<Long>() {
                @Override
                protected Long executeProbeOperation(ProcessProbe probe) throws UnsupportedOperationException {
                    return probe.processUserTime();
                }
            });
        }

        @Override
        public Long residentMemorySize() {
            return executeOnProbes(new Callback<Long>() {
                @Override
                protected Long executeProbeOperation(ProcessProbe probe) throws UnsupportedOperationException {
                    return probe.residentMemorySize();
                }
            });
        }

        @Override
        public Long sharedMemorySize() {
            return executeOnProbes(new Callback<Long>() {
                @Override
                protected Long executeProbeOperation(ProcessProbe probe) throws UnsupportedOperationException {
                    return probe.sharedMemorySize();
                }
            });
        }
    }
}
