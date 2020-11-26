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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        final Map<String, JvmOption> finalJvmOptions = finalJvmOptions(userDefinedJvmOptions);
        final long heapSize = extractHeapSize(finalJvmOptions);
        final long maxDirectMemorySize = extractMaxDirectMemorySize(finalJvmOptions);
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

    private static final Pattern OPTION = Pattern.compile(
        "^\\s*\\S+\\s+(?<flag>\\S+)\\s+:?=\\s+(?<value>\\S+)?\\s+\\{[^}]+?\\}\\s+\\{(?<origin>[^}]+)}"
    );

    private static class JvmOption {
        private final String value;
        private final String origin;

        JvmOption(String value, String origin) {
            this.value = value;
            this.origin = origin;
        }

        public Optional<String> getValue() {
            return Optional.ofNullable(value);
        }

        public String getMandatoryValue() {
            return value;
        }

        public boolean isCommandLineOrigin() {
            return "command line".equals(this.origin);
        }
    }

    static Map<String, JvmOption> finalJvmOptions(final List<String> userDefinedJvmOptions) throws InterruptedException, IOException {
        return flagsFinal(userDefinedJvmOptions).stream()
            .map(OPTION::matcher)
            .filter(Matcher::matches)
            .collect(Collectors.toUnmodifiableMap(m -> m.group("flag"), m -> new JvmOption(m.group("value"), m.group("origin"))));
    }

    private static List<String> flagsFinal(final List<String> userDefinedJvmOptions) throws InterruptedException, IOException {
        /*
         * To deduce the final set of JVM options that Elasticsearch is going to start with, we start a separate Java process with the JVM
         * options that we would pass on the command line. For this Java process we will add two additional flags, -XX:+PrintFlagsFinal and
         * -version. This causes the Java process that we start to parse the JVM options into their final values, display them on standard
         * output, print the version to standard error, and then exit. The JVM itself never bootstraps, and therefore this process is
         * lightweight. By doing this, we get the JVM options parsed exactly as the JVM that we are going to execute would parse them
         * without having to implement our own JVM option parsing logic.
         */
        final String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        final List<String> command = Stream.of(
            Stream.of(java),
            userDefinedJvmOptions.stream(),
            Stream.of("-Xshare:off"),
            Stream.of("-XX:+PrintFlagsFinal"),
            Stream.of("-version")
        ).reduce(Stream::concat).get().collect(Collectors.toUnmodifiableList());
        final Process process = new ProcessBuilder().command(command).start();
        final List<String> output = readLinesFromInputStream(process.getInputStream());
        final List<String> error = readLinesFromInputStream(process.getErrorStream());
        final int status = process.waitFor();
        if (status != 0) {
            final String message = String.format(
                Locale.ROOT,
                "starting java failed with [%d]\noutput:\n%s\nerror:\n%s",
                status,
                String.join("\n", output),
                String.join("\n", error)
            );
            throw new RuntimeException(message);
        } else {
            return output;
        }
    }

    private static List<String> readLinesFromInputStream(final InputStream is) throws IOException {
        try (InputStreamReader isr = new InputStreamReader(is, StandardCharsets.UTF_8); BufferedReader br = new BufferedReader(isr)) {
            return br.lines().collect(Collectors.toUnmodifiableList());
        }
    }

    // package private for testing
    static Long extractHeapSize(final Map<String, JvmOption> finalJvmOptions) {
        return Long.parseLong(finalJvmOptions.get("MaxHeapSize").getMandatoryValue());
    }

    static long extractMaxDirectMemorySize(final Map<String, JvmOption> finalJvmOptions) {
        return Long.parseLong(finalJvmOptions.get("MaxDirectMemorySize").getMandatoryValue());
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
