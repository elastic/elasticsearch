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

package org.elasticsearch.bootstrap;

import org.apache.lucene.util.Constants;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Checks that the JVM is ok and won't cause index corruption */
final class JVMCheck {

    /** no instantiation */
    private JVMCheck() {}

    /**
     * System property which if set causes us to bypass the check completely (but issues a warning in doing so)
     */
    static final String JVM_BYPASS = "es.bypass.vm.check";

    /**
     * Checks that the current JVM is "ok". This means it doesn't have severe bugs that cause data corruption.
     */
    static void check() {
        if (Boolean.parseBoolean(System.getProperty(JVM_BYPASS))) {
            Loggers.getLogger(JVMCheck.class).warn("bypassing jvm version check for version [{}], this can result in data corruption!", fullVersion());
        } else if ("IBM Corporation".equals(Constants.JVM_VENDOR)) {
            // currently some old JVM versions from IBM will easily result in index corruption.
            // 2.8+ seems ok for ES from testing.
            float version = Float.POSITIVE_INFINITY;
            try {
                version = Float.parseFloat(Constants.JVM_VERSION);
            } catch (NumberFormatException ignored) {
                // this is just a simple best-effort to detect old runtimes,
                // if we cannot parse it, we don't fail.
            }
            if (version < 2.8f) {
                StringBuilder sb = new StringBuilder();
                sb.append("IBM J9 runtimes < 2.8 suffer from several bugs which can cause data corruption.");
                sb.append(System.lineSeparator());
                sb.append("Your version: " + fullVersion());
                sb.append(System.lineSeparator());
                sb.append("Please upgrade the JVM to a recent IBM JDK");
                throw new RuntimeException(sb.toString());
            }
        }
    }

    /**
     * Returns java + jvm version, looks like this:
     * {@code Oracle Corporation 1.8.0_45 [Java HotSpot(TM) 64-Bit Server VM 25.45-b02]}
     */
    static String fullVersion() {
        StringBuilder sb = new StringBuilder();
        sb.append(Constants.JAVA_VENDOR);
        sb.append(" ");
        sb.append(Constants.JAVA_VERSION);
        sb.append(" [");
        sb.append(Constants.JVM_NAME);
        sb.append(" ");
        sb.append(Constants.JVM_VERSION);
        sb.append("]");
        return sb.toString();
    }

}
