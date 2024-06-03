/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.telemetry.apm.jvm;

import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import javax.annotation.Nullable;

public class JvmFdMetrics /*extends AbstractLifecycleListener */{

    // using method handles to avoid direct reference to `com.sun.*` classes which we can't reference directly
    // as it is not available on all JDKs this is further enforced by the animal sniffer plugin
    private static final MethodHandle NOOP = MethodHandles.constant(String.class, "no-op");
    private static final MethodHandle getOpenFileDescriptorCount = getMethodHandle("getOpenFileDescriptorCount");
    private static final MethodHandle getMaxFileDescriptorCount = getMethodHandle("getMaxFileDescriptorCount");

    public void start(TelemetryProvider tracer) {
        bindTo(tracer.getMeterRegistry());
    }

    void bindTo(final MeterRegistry registry) {
        final OperatingSystemMXBean mxBean = ManagementFactory.getOperatingSystemMXBean();

        Class<?> targetClass = getTargetClass();
        if (targetClass == null || !targetClass.isAssignableFrom(mxBean.getClass())) {
            return;
        }

        register(registry, "jvm.fd.used", mxBean, getOpenFileDescriptorCount);
        register(registry, "jvm.fd.max", mxBean, getMaxFileDescriptorCount);
    }

    private static void register(MeterRegistry registry, String name, final OperatingSystemMXBean mxBean, final MethodHandle methodHandle) {
        if (methodHandle == NOOP) {
            return;
        }
        registry.registerLongGauge("es."+name+".total", "","count",()-> {
            return new LongWithAttributes(getLongWithAttributes(mxBean, methodHandle));
            }
        );

    }

    private static long getLongWithAttributes(OperatingSystemMXBean mxBean, MethodHandle methodHandle) {
        try {
            return (long) methodHandle.invoke(mxBean);
        } catch (Throwable e) {
            return -1L;
        }
    }

    @Nullable
    private static Class<?> getTargetClass() {
        try {
            return Class.forName("com.sun.management.UnixOperatingSystemMXBean");
        } catch (ClassNotFoundException e) {
            return null;
        }
    }

    private static MethodHandle getMethodHandle(String name) {
        Class<?> targetClass = getTargetClass();
        if (targetClass == null) {
            return NOOP;
        }
        try {
            return MethodHandles.lookup().findVirtual(targetClass, name, MethodType.methodType(long.class));
        } catch (Exception e) {
            return NOOP;
        }
    }
}
