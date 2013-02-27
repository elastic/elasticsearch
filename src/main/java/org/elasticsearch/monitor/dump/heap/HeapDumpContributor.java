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

package org.elasticsearch.monitor.dump.heap;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.monitor.dump.Dump;
import org.elasticsearch.monitor.dump.DumpContributionFailedException;
import org.elasticsearch.monitor.dump.DumpContributor;

import java.lang.reflect.Method;

/**
 *
 */
public class HeapDumpContributor implements DumpContributor {

    public static final String HEAP_DUMP = "heap";

    private final Method heapDumpMethod;
    private final Object diagnosticMBean;

    private final String name;

    @Inject
    public HeapDumpContributor(@Assisted String name, @Assisted Settings settings) {
        this.name = name;
        Method heapDumpMethod;
        Object diagnosticMBean;
        try {
            Class managementFactoryClass = Class.forName("sun.management.ManagementFactory", true, HeapDumpContributor.class.getClassLoader());
            Method method = managementFactoryClass.getMethod("getDiagnosticMXBean");
            diagnosticMBean = method.invoke(null);
            heapDumpMethod = diagnosticMBean.getClass().getMethod("dumpHeap", String.class, boolean.class);
        } catch (Exception _ex) {
            heapDumpMethod = null;
            diagnosticMBean = null;
        }
        this.heapDumpMethod = heapDumpMethod;
        this.diagnosticMBean = diagnosticMBean;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void contribute(Dump dump) throws DumpContributionFailedException {
        if (heapDumpMethod == null) {
            throw new DumpContributionFailedException(getName(), "Heap dump not enabled on this JVM");
        }
        try {
            heapDumpMethod.invoke(diagnosticMBean, dump.createFile("heap.hprof").getAbsolutePath(), true);
        } catch (Exception e) {
            throw new DumpContributionFailedException(getName(), "Failed to generate heap dump", e);
        }
    }
}
