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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Tunes Elasticsearch JVM settings based on inspection of provided JVM options.
 */
final class JvmErgonomics {
    private static final long KB = 1024L;

    private static final long MB = 1024L * 1024L;

    private static final long GB = 1024L * 1024L * 1024L;


    private JvmErgonomics() {
        throw new AssertionError("No instances intended");
    }

    /**
     * Chooses additional JVM options for Elasticsearch.
     *
     * @param userDefinedJvmOptions A list of JVM options that have been defined by the user.
     * @return A list of additional JVM options to set.
     */
    static List<String> choose(List<String> userDefinedJvmOptions) {
        List<String> ergonomicChoices = new ArrayList<>();
        Long heapSize = extractHeapSize(userDefinedJvmOptions);
        Map<String, String> systemProperties = extractSystemProperties(userDefinedJvmOptions);
        if (heapSize != null) {
            if (systemProperties.containsKey("io.netty.allocator.type") == false) {
                if (heapSize <= 1 * GB) {
                    ergonomicChoices.add("-Dio.netty.allocator.type=unpooled");
                } else {
                    ergonomicChoices.add("-Dio.netty.allocator.type=pooled");
                }
            }
        }
        return ergonomicChoices;
    }

    private static final Pattern MAX_HEAP_SIZE = Pattern.compile("^(-Xmx|-XX:MaxHeapSize=)(?<size>\\d+)(?<unit>\\w)?$");

    // package private for testing
    static Long extractHeapSize(List<String> userDefinedJvmOptions) {
        for (String jvmOption : userDefinedJvmOptions) {
            final Matcher matcher = MAX_HEAP_SIZE.matcher(jvmOption);
            if (matcher.matches()) {
                final long size = Long.parseLong(matcher.group("size"));
                final String unit = matcher.group("unit");
                if (unit == null) {
                    return size;
                } else {
                    switch (unit.toLowerCase(Locale.ROOT)) {
                        case "k":
                            return size * KB;
                        case "m":
                            return size * MB;
                        case "g":
                            return size * GB;
                        default:
                            throw new IllegalArgumentException("Unknown unit [" + unit + "] for max heap size in [" + jvmOption + "]");
                    }
                }
            }
        }
        return null;
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
