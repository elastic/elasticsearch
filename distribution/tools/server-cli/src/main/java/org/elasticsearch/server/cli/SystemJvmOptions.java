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
import org.elasticsearch.jdk.RuntimeVersionFeature;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

final class SystemJvmOptions {

    static List<String> systemJvmOptions(Settings nodeSettings, final Map<String, String> sysprops) {
        Path esHome = Path.of(sysprops.get("es.path.home"));
        String distroType = sysprops.get("es.distribution.type");
        String javaType = sysprops.get("es.java.type");
        boolean isHotspot = sysprops.getOrDefault("sun.management.compiler", "").contains("HotSpot");

        boolean useEntitlements = true;
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
                // temporary until we get off-heap vector stats in Lucene 10.3
                "--add-opens=org.apache.lucene.core/org.apache.lucene.codecs.lucene99=org.elasticsearch.server",
                "--add-opens=org.apache.lucene.backward_codecs/org.apache.lucene.backward_codecs.lucene90=org.elasticsearch.server",
                "--add-opens=org.apache.lucene.backward_codecs/org.apache.lucene.backward_codecs.lucene91=org.elasticsearch.server",
                "--add-opens=org.apache.lucene.backward_codecs/org.apache.lucene.backward_codecs.lucene92=org.elasticsearch.server",
                "--add-opens=org.apache.lucene.backward_codecs/org.apache.lucene.backward_codecs.lucene94=org.elasticsearch.server",
                "--add-opens=org.apache.lucene.backward_codecs/org.apache.lucene.backward_codecs.lucene95=org.elasticsearch.server",
                // log4j 2
                "-Dlog4j.shutdownHookEnabled=false",
                "-Dlog4j2.disable.jmx=true",
                "-Dlog4j2.formatMsgNoLookups=true",
                "-Djava.locale.providers=CLDR",
                // Enable vectorization for whatever version we are running. This ensures we use vectorization even when running EA builds.
                "-Dorg.apache.lucene.vectorization.upperJavaFeatureVersion=" + Runtime.version().feature(),
                // Pass through some properties
                "-Des.path.home=" + esHome,
                "-Des.distribution.type=" + distroType,
                "-Des.java.type=" + javaType
            ),
            maybeEnableNativeAccess(useEntitlements),
            maybeOverrideDockerCgroup(distroType),
            maybeSetActiveProcessorCount(nodeSettings),
            maybeSetReplayFile(distroType, isHotspot),
            maybeWorkaroundG1Bug(),
            maybeAllowSecurityManager(useEntitlements),
            maybeAttachEntitlementAgent(esHome, useEntitlements)
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

    private static Stream<String> maybeEnableNativeAccess(boolean useEntitlements) {
        var enableNativeAccessOptions = new ArrayList<String>();
        if (Runtime.version().feature() >= 21) {
            enableNativeAccessOptions.add("--enable-native-access=org.elasticsearch.nativeaccess,org.apache.lucene.core");
            if (useEntitlements) {
                enableNativeAccessOptions.add("--enable-native-access=ALL-UNNAMED");
                if (Runtime.version().feature() >= 24) {
                    enableNativeAccessOptions.add("--illegal-native-access=deny");
                }
            }
        }
        return enableNativeAccessOptions.stream();
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

    private static Stream<String> maybeAllowSecurityManager(boolean useEntitlements) {
        if (RuntimeVersionFeature.isSecurityManagerAvailable()) {
            // Will become conditional on useEntitlements once entitlements can run without SM
            return Stream.of("-Djava.security.manager=allow");
        }
        return Stream.of();
    }

    private static Stream<String> maybeAttachEntitlementAgent(Path esHome, boolean useEntitlements) {
        if (useEntitlements == false) {
            return Stream.empty();
        }

        Path dir = esHome.resolve("lib/entitlement-bridge");
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
        // We instrument classes in these modules to call the bridge. Because the bridge gets patched
        // into java.base, we must export the bridge from java.base to these modules, as a comma-separated list
        String modulesContainingEntitlementInstrumentation = "java.logging,java.net.http,java.naming,jdk.net";
        return Stream.of(
            "-Des.entitlements.enabled=true",
            "-XX:+EnableDynamicAgentLoading",
            "-Djdk.attach.allowAttachSelf=true",
            "--patch-module=java.base=" + bridgeJar,
            "--add-exports=java.base/org.elasticsearch.entitlement.bridge=org.elasticsearch.entitlement,"
                + modulesContainingEntitlementInstrumentation
        );
    }
}
