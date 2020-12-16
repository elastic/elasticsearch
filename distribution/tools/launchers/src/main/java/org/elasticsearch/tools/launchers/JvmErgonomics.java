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

package org.elasticsearch.tools.launchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Tunes Elasticsearch JVM settings based on inspection of provided JVM options.
 */
final class JvmErgonomics {

    private JvmErgonomics() {
        throw new AssertionError("No instances intended");
    }

    /**
     * Chooses additional JVM options for Elasticsearch.
     *
     * @param userDefinedJvmOptions A list of JVM options that have been defined by the user.
     * @return A list of additional JVM options to set.
     */
    static List<String> choose(final List<String> userDefinedJvmOptions) throws InterruptedException, IOException {
        final List<String> ergonomicChoices = new ArrayList<>();
        final Map<String, JvmOption> finalJvmOptions = JvmOption.findFinalOptions(userDefinedJvmOptions);
        final long heapSize = JvmOption.extractMaxHeapSize(finalJvmOptions);
        final long maxDirectMemorySize = JvmOption.extractMaxDirectMemorySize(finalJvmOptions);
        if (maxDirectMemorySize == 0) {
            ergonomicChoices.add("-XX:MaxDirectMemorySize=" + heapSize / 2);
        }

        final boolean tuneG1GCForSmallHeap = tuneG1GCForSmallHeap(heapSize);
        final boolean tuneG1GCHeapRegion = tuneG1GCHeapRegion(finalJvmOptions, tuneG1GCForSmallHeap);
        final boolean tuneG1GCInitiatingHeapOccupancyPercent = tuneG1GCInitiatingHeapOccupancyPercent(finalJvmOptions);
        final int tuneG1GCReservePercent = tuneG1GCReservePercent(finalJvmOptions, tuneG1GCForSmallHeap);

        if (tuneG1GCHeapRegion) {
            ergonomicChoices.add("-XX:G1HeapRegionSize=4m");
        }
        if (tuneG1GCInitiatingHeapOccupancyPercent) {
            ergonomicChoices.add("-XX:InitiatingHeapOccupancyPercent=30");
        }
        if (tuneG1GCReservePercent != 0) {
            ergonomicChoices.add("-XX:G1ReservePercent=" + tuneG1GCReservePercent);
        }

        return ergonomicChoices;
    }

    // Tune G1GC options for heaps < 8GB
    static boolean tuneG1GCForSmallHeap(final long heapSize) {
        return heapSize < 8L << 30;
    }

    static boolean tuneG1GCHeapRegion(final Map<String, JvmOption> finalJvmOptions, final boolean tuneG1GCForSmallHeap) {
        JvmOption g1GCHeapRegion = finalJvmOptions.get("G1HeapRegionSize");
        JvmOption g1GC = finalJvmOptions.get("UseG1GC");
        return (tuneG1GCForSmallHeap && g1GC.getMandatoryValue().equals("true") && g1GCHeapRegion.isCommandLineOrigin() == false);
    }

    static int tuneG1GCReservePercent(final Map<String, JvmOption> finalJvmOptions, final boolean tuneG1GCForSmallHeap) {
        JvmOption g1GC = finalJvmOptions.get("UseG1GC");
        JvmOption g1GCReservePercent = finalJvmOptions.get("G1ReservePercent");
        if (g1GC.getMandatoryValue().equals("true")) {
            if (g1GCReservePercent.isCommandLineOrigin() == false && tuneG1GCForSmallHeap) {
                return 15;
            } else if (g1GCReservePercent.isCommandLineOrigin() == false && tuneG1GCForSmallHeap == false) {
                return 25;
            }
        }
        return 0;
    }

    static boolean tuneG1GCInitiatingHeapOccupancyPercent(final Map<String, JvmOption> finalJvmOptions) {
        JvmOption g1GC = finalJvmOptions.get("UseG1GC");
        JvmOption g1GCInitiatingHeapOccupancyPercent = finalJvmOptions.get("InitiatingHeapOccupancyPercent");
        return g1GCInitiatingHeapOccupancyPercent.isCommandLineOrigin() == false && g1GC.getMandatoryValue().equals("true");
    }

    private static final Pattern SYSTEM_PROPERTY = Pattern.compile("^-D(?<key>[\\w+].*?)=(?<value>.*)$");

    // package private for testing
    static Map<String, String> extractSystemProperties(List<String> userDefinedJvmOptions) {
        Map<String, String> systemProperties = new HashMap<>();
        for (String jvmOption : userDefinedJvmOptions) {
            final Matcher matcher = SYSTEM_PROPERTY.matcher(jvmOption);
            if (matcher.matches()) {
                systemProperties.put(matcher.group("key"), matcher.group("value"));
            }
        }
        return systemProperties;
    }

}
