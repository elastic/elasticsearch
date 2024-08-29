/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class SystemJvmOptions {

    static List<String> systemJvmOptions(Settings nodeSettings, final Map<String, String> sysprops) {
        String distroType = sysprops.get("es.distribution.type");
        boolean isHotspot = sysprops.getOrDefault("sun.management.compiler", "").contains("HotSpot");

        return Stream.concat(
            Stream.of(
                /*
                 * Cache ttl in seconds for positive DNS lookups noting that this overrides the JDK security property
                 * networkaddress.cache.ttl can be set to -1 to cache forever.
                 */
                "-Des.networkaddress.cache.ttl=60",
                /*
                 * Cache ttl in seconds for negative DNS lookups noting that this overrides the JDK security property
                 * networkaddress.cache.negative ttl; set to -1 to cache forever.
                 */
                "-Des.networkaddress.cache.negative.ttl=10",
                // Allow to set the security manager.
                "-Djava.security.manager=allow",
                // pre-touch JVM emory pages during initialization
                "-XX:+AlwaysPreTouch",
                // explicitly set the stack size
                "-Xss1m",
                // set to headless, just in case,
                "-Djava.awt.headless=true",
                // ensure UTF-8 encoding by default (e.g., filenames)
                "-Dfile.encoding=UTF-8",
                // use our provided JNA always versus the system one
                "-Djna.nosys=true",
                /*
                 * Turn off a JDK optimization that throws away stack traces for common exceptions because stack traces are important for
                 * debugging.
                 */
                "-XX:-OmitStackTraceInFastThrow",
                // flags to configure Netty
                "-Dio.netty.noUnsafe=true",
                "-Dio.netty.noKeySetOptimization=true",
                "-Dio.netty.recycler.maxCapacityPerThread=0",
                // log4j 2
                "-Dlog4j.shutdownHookEnabled=false",
                "-Dlog4j2.disable.jmx=true",
                "-Dlog4j2.formatMsgNoLookups=true",
                /*
                 * Due to internationalization enhancements in JDK 9 Elasticsearch need to set the provider to COMPAT otherwise time/date
                 * parsing will break in an incompatible way for some date patterns and locales.
                 */
                "-Djava.locale.providers=SPI,COMPAT",
                maybeEnableNativeAccess(),
                maybeOverrideDockerCgroup(distroType),
                maybeSetActiveProcessorCount(nodeSettings),
                setReplayFile(distroType, isHotspot),
                // Pass through distribution type
                "-Des.distribution.type=" + distroType
            ),
            maybeWorkaroundG1Bug()
        ).filter(e -> e.isEmpty() == false).collect(Collectors.toList());
    }

    /*
     * The virtual file /proc/self/cgroup should list the current cgroup
     * membership. For each hierarchy, you can follow the cgroup path from
     * this file to the cgroup filesystem (usually /sys/fs/cgroup/) and
     * introspect the statistics for the cgroup for the given
     * hierarchy. Alas, Docker breaks this by mounting the container
     * statistics at the root while leaving the cgroup paths as the actual
     * paths. Therefore, Elasticsearch provides a mechanism to override
     * reading the cgroup path from /proc/self/cgroup and instead uses the
     * cgroup path defined the JVM system property
     * es.cgroups.hierarchy.override. Therefore, we set this value here so
     * that cgroup statistics are available for the container this process
     * will run in.
     */
    private static String maybeOverrideDockerCgroup(String distroType) {
        if ("docker".equals(distroType)) {
            return "-Des.cgroups.hierarchy.override=/";
        }
        return "";
    }

    private static String setReplayFile(String distroType, boolean isHotspot) {
        if (isHotspot == false) {
            // the replay file option is only guaranteed for hotspot vms
            return "";
        }
        String replayDir = "logs";
        if ("rpm".equals(distroType) || "deb".equals(distroType)) {
            replayDir = "/var/log/elasticsearch";
        }
        return "-XX:ReplayDataFile=" + replayDir + "/replay_pid%p.log";
    }

    /*
     * node.processors determines thread pool sizes for Elasticsearch. When it
     * is set, we need to also tell the JVM to respect a different value
     */
    private static String maybeSetActiveProcessorCount(Settings nodeSettings) {
        if (EsExecutors.NODE_PROCESSORS_SETTING.exists(nodeSettings)) {
            int allocated = EsExecutors.allocatedProcessors(nodeSettings);
            return "-XX:ActiveProcessorCount=" + allocated;
        }
        return "";
    }

    private static String maybeEnableNativeAccess() {
        if (Runtime.version().feature() >= 21) {
            return "--enable-native-access=org.elasticsearch.nativeaccess,org.apache.lucene.core";
        }
        return "";
    }

    /*
     * Only affects 22 and 22.0.1, see https://bugs.openjdk.org/browse/JDK-8329528
     */
    private static Stream<String> maybeWorkaroundG1Bug() {
        Runtime.Version v = Runtime.version();
        if (v.feature() == 22 && v.update() <= 1) {
            return Stream.of("-XX:+UnlockDiagnosticVMOptions", "-XX:G1NumCollectionsKeepPinned=10000000");
        }
        return Stream.of();
    }
}
