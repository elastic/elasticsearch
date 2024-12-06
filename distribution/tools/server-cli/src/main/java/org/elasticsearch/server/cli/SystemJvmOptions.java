/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.jdk.RuntimeVersionFeature;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

final class SystemJvmOptions {

    static List<String> systemJvmOptions(Settings nodeSettings, final Map<String, String> sysprops) {
        String distroType = sysprops.get("es.distribution.type");
        boolean isHotspot = sysprops.getOrDefault("sun.management.compiler", "").contains("HotSpot");
        boolean useEntitlements = Boolean.parseBoolean(sysprops.getOrDefault("es.entitlements.enabled", "false"));
        return Stream.of(
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
                "-Djava.locale.providers=CLDR",
                // Pass through distribution type
                "-Des.distribution.type=" + distroType
            ),
            maybeEnableNativeAccess(),
            maybeOverrideDockerCgroup(distroType),
            maybeSetActiveProcessorCount(nodeSettings),
            maybeSetReplayFile(distroType, isHotspot),
            maybeWorkaroundG1Bug(),
            maybeAllowSecurityManager(),
            maybeAttachEntitlementAgent(useEntitlements)
        ).flatMap(s -> s).toList();
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
    private static Stream<String> maybeOverrideDockerCgroup(String distroType) {
        if ("docker".equals(distroType)) {
            return Stream.of("-Des.cgroups.hierarchy.override=/");
        }
        return Stream.empty();
    }

    private static Stream<String> maybeSetReplayFile(String distroType, boolean isHotspot) {
        if (isHotspot == false) {
            // the replay file option is only guaranteed for hotspot vms
            return Stream.empty();
        }
        String replayDir = "logs";
        if ("rpm".equals(distroType) || "deb".equals(distroType)) {
            replayDir = "/var/log/elasticsearch";
        }
        return Stream.of("-XX:ReplayDataFile=" + replayDir + "/replay_pid%p.log");
    }

    /*
     * node.processors determines thread pool sizes for Elasticsearch. When it
     * is set, we need to also tell the JVM to respect a different value
     */
    private static Stream<String> maybeSetActiveProcessorCount(Settings nodeSettings) {
        if (EsExecutors.NODE_PROCESSORS_SETTING.exists(nodeSettings)) {
            int allocated = EsExecutors.allocatedProcessors(nodeSettings);
            return Stream.of("-XX:ActiveProcessorCount=" + allocated);
        }
        return Stream.empty();
    }

    private static Stream<String> maybeEnableNativeAccess() {
        if (Runtime.version().feature() >= 21) {
            return Stream.of("--enable-native-access=org.elasticsearch.nativeaccess,org.apache.lucene.core");
        }
        return Stream.empty();
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

    @UpdateForV9(owner = UpdateForV9.Owner.CORE_INFRA)
    private static Stream<String> maybeAllowSecurityManager() {
        if (RuntimeVersionFeature.isSecurityManagerAvailable()) {
            // Will become conditional on useEntitlements once entitlements can run without SM
            return Stream.of("-Djava.security.manager=allow");
        }
        return Stream.of();
    }

    private static Stream<String> maybeAttachEntitlementAgent(boolean useEntitlements) {
        if (useEntitlements == false) {
            return Stream.empty();
        }

        Path dir = Path.of("lib", "entitlement-bridge");
        if (Files.exists(dir) == false) {
            throw new IllegalStateException("Directory for entitlement bridge jar does not exist: " + dir);
        }
        String bridgeJar;
        try (var s = Files.list(dir)) {
            var candidates = s.limit(2).toList();
            if (candidates.size() != 1) {
                throw new IllegalStateException("Expected one jar in " + dir + "; found " + candidates.size());
            }
            bridgeJar = candidates.get(0).toString();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to list entitlement jars in: " + dir, e);
        }
        return Stream.of(
            "-Des.entitlements.enabled=true",
            "-XX:+EnableDynamicAgentLoading",
            "-Djdk.attach.allowAttachSelf=true",
            "--patch-module=java.base=" + bridgeJar,
            "--add-exports=java.base/org.elasticsearch.entitlement.bridge=org.elasticsearch.entitlement"
        );
    }
}
