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

package org.elasticsearch.monitor.probe;

import org.apache.lucene.util.Constants;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.monitor.process.ProcessProbe;

import java.lang.reflect.Method;

import static org.elasticsearch.monitor.jvm.JvmInfo.jvmInfo;

public class ProbeUtils {

    /**
     * Use reflection to calls every method on all probes of a ProbeRegistry (useful for investigation).
     */
    public static <P extends Probe> void logProbesResults(ESLogger logger, ProbeRegistry<P> registry) {
        logger.trace("{} - {} {} ({})", Constants.OS_NAME, jvmInfo().getVmVendor(), jvmInfo().getVmName(), jvmInfo().getVersion());

        StringBuilder builder = new StringBuilder("Method;");
        for (Probe probe : registry.probes()) {
            builder.append(probe.getClass().getSimpleName()).append(';');
        }
        logger.trace("{}", builder.toString());
        builder.setLength(0);

        for (Method method : ProcessProbe.class.getDeclaredMethods()) {
            builder.append(method.getName()).append(';');

            for (Probe probe : registry.probes()) {
                Object result = null;
                try {
                    result = method.invoke(probe);
                } catch (Exception e) {
                    if (e instanceof UnsupportedOperationException) {
                        builder.append("n/a");
                    }
                }

                if (result != null) {
                    builder.append(result);

                }
                builder.append(';');
            }
            logger.trace("{}", builder.toString());
            builder.setLength(0);
        }
    }
}
