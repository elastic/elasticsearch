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

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Checks that the JVM is ok and won't cause index corruption */
final class JVMCheck {
    /** no instantiation */
    private JVMCheck() {}
    
    /**
     * URL with latest JVM recommendations
     */
    static final String JVM_RECOMMENDATIONS = "http://www.elastic.co/guide/en/elasticsearch/reference/current/_installation.html";

    /**
     * System property which if set causes us to bypass the check completely (but issues a warning in doing so)
     */
    static final String JVM_BYPASS = "es.bypass.vm.check";

    /**
     * Metadata and messaging for hotspot bugs.
     */
    static final class HotspotBug {
        
        /** OpenJDK bug URL */
        final String bugUrl;
        
        /** Compiler workaround flag (null if there is no workaround) */
        final String workAround;
        
        HotspotBug(String bugUrl, String workAround) {
            this.bugUrl = bugUrl;
            this.workAround = workAround;
        }
        
        /** Returns an error message to the user for a broken version */
        String getErrorMessage() {
            StringBuilder sb = new StringBuilder();
            sb.append("Java version: ").append(fullVersion());
            sb.append(" suffers from critical bug ").append(bugUrl);
            sb.append(" which can cause data corruption.");
            sb.append(System.lineSeparator());
            sb.append("Please upgrade the JVM, see ").append(JVM_RECOMMENDATIONS);
            sb.append(" for current recommendations.");
            if (workAround != null) {
                sb.append(System.lineSeparator());
                sb.append("If you absolutely cannot upgrade, please add ").append(workAround);
                sb.append(" to the JAVA_OPTS environment variable.");
                sb.append(System.lineSeparator());
                sb.append("Upgrading is preferred, this workaround will result in degraded performance.");
            }
            return sb.toString();
        }
        
        /** Warns the user when a workaround is being used to dodge the bug */
        String getWarningMessage() {
            StringBuilder sb = new StringBuilder();
            sb.append("Workaround flag ").append(workAround);
            sb.append(" for bug ").append(bugUrl);
            sb.append(" found. ");
            sb.append(System.lineSeparator());
            sb.append("This will result in degraded performance!");
            sb.append(System.lineSeparator());
            sb.append("Upgrading is preferred, see ").append(JVM_RECOMMENDATIONS);
            sb.append(" for current recommendations.");
            return sb.toString();
        }
    }
    
    /** mapping of hotspot version to hotspot bug information for the most serious bugs */
    static final Map<String,HotspotBug> JVM_BROKEN_HOTSPOT_VERSIONS;
    
    static {
        Map<String,HotspotBug> bugs = new HashMap<>();
        
        // 1.7.0: loop optimizer bug
        bugs.put("21.0-b17",  new HotspotBug("https://bugs.openjdk.java.net/browse/JDK-7070134", "-XX:-UseLoopPredicate"));
        // register allocation issues (technically only x86/amd64). This impacted update 40, 45, and 51
        bugs.put("24.0-b56",  new HotspotBug("https://bugs.openjdk.java.net/browse/JDK-8024830", "-XX:-UseSuperWord"));
        bugs.put("24.45-b08", new HotspotBug("https://bugs.openjdk.java.net/browse/JDK-8024830", "-XX:-UseSuperWord"));
        bugs.put("24.51-b03", new HotspotBug("https://bugs.openjdk.java.net/browse/JDK-8024830", "-XX:-UseSuperWord"));
        
        JVM_BROKEN_HOTSPOT_VERSIONS = Collections.unmodifiableMap(bugs);
    }

    /**
     * Checks that the current JVM is "ok". This means it doesn't have severe bugs that cause data corruption.
     */
    static void check() {
        if (Boolean.parseBoolean(System.getProperty(JVM_BYPASS))) {
            Loggers.getLogger(JVMCheck.class).warn("bypassing jvm version check for version [{}], this can result in data corruption!", fullVersion());
        } else if ("Oracle Corporation".equals(Constants.JVM_VENDOR)) {
            HotspotBug bug = JVM_BROKEN_HOTSPOT_VERSIONS.get(Constants.JVM_VERSION);
            if (bug != null) {
                if (bug.workAround != null && ManagementFactory.getRuntimeMXBean().getInputArguments().contains(bug.workAround)) {
                    Loggers.getLogger(JVMCheck.class).warn(bug.getWarningMessage());
                } else {
                    throw new RuntimeException(bug.getErrorMessage());
                }
            }
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
