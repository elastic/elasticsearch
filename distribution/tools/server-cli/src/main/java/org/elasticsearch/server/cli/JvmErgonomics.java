/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.server.cli;

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
        return tuneG1GCForSmallHeap && usingG1GcWithoutCommandLineOriginOption(finalJvmOptions, "G1HeapRegionSize");
    }

    static int tuneG1GCReservePercent(final Map<String, JvmOption> finalJvmOptions, final boolean tuneG1GCForSmallHeap) {
        if (usingG1GcWithoutCommandLineOriginOption(finalJvmOptions, "G1ReservePercent")) {
            return tuneG1GCForSmallHeap ? 15 : 25;
        }
        return 0;
    }

    static boolean tuneG1GCInitiatingHeapOccupancyPercent(final Map<String, JvmOption> finalJvmOptions) {
        return usingG1GcWithoutCommandLineOriginOption(finalJvmOptions, "InitiatingHeapOccupancyPercent");
    }

    /**
     * @return <ul>
     *         <li>{@code true} if `-XX:+UseG1GC` is in the final JVM options and {@code optionName} was not specified.
     *         <li>{@code false} if either `-XX:-UseG1GC` is in the final JVM options, or {@code optionName} was specified.
     *         </ul>
     *
     * @throws IllegalStateException if neither `-XX:+UseG1GC` nor `-XX:-UseG1GC` is in the final JVM options, or `-XX:+UseG1GC` is selected
     *                               and {@code optionName} is not in the final JVM options.
     */
    private static boolean usingG1GcWithoutCommandLineOriginOption(Map<String, JvmOption> finalJvmOptions, String optionName) {
        return getRequiredOption(finalJvmOptions, "UseG1GC").getMandatoryValue().equals("true")
            && getRequiredOption(finalJvmOptions, optionName).isCommandLineOrigin() == false;
    }

    private static JvmOption getRequiredOption(final Map<String, JvmOption> finalJvmOptions, final String key) {
        final var jvmOption = finalJvmOptions.get(key);
        if (jvmOption == null) {
            throw new IllegalStateException(
                "JVM option [" + key + "] was unexpectedly missing. Elasticsearch requires this option to be present."
            );
        }
        return jvmOption;
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
