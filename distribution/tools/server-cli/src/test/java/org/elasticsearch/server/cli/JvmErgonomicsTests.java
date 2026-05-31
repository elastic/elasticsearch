/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.server.cli;

import org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressFileSystems("*")
public class JvmErgonomicsTests extends ESTestCase {

    public void testExtractValidHeapSizeUsingXmx() throws Exception {
        assertThat(JvmOption.extractMaxHeapSize(JvmOption.findFinalOptions(Collections.singletonList("-Xmx2g"))), equalTo(2L << 30));
    }

    public void testExtractValidHeapSizeUsingMaxHeapSize() throws Exception {
        assertThat(
            JvmOption.extractMaxHeapSize(JvmOption.findFinalOptions(Collections.singletonList("-XX:MaxHeapSize=2g"))),
            equalTo(2L << 30)
        );
    }

    public void testExtractValidHeapSizeNoOptionPresent() throws Exception {
        assertThat(JvmOption.extractMaxHeapSize(JvmOption.findFinalOptions(Collections.emptyList())), greaterThan(0L));
    }

    public void testHeapSizeInvalid() throws InterruptedException, IOException {
        try {
            JvmOption.extractMaxHeapSize(JvmOption.findFinalOptions(Collections.singletonList("-Xmx2Z")));
            fail("expected starting java to fail");
        } catch (final RuntimeException e) {
            assertThat(e, hasToString(containsString(("starting java failed"))));
            assertThat(e, hasToString(containsString(("Invalid maximum heap size: -Xmx2Z"))));
        }
    }

    public void testHeapSizeTooSmall() throws Exception {
        try {
            JvmOption.extractMaxHeapSize(JvmOption.findFinalOptions(Collections.singletonList("-Xmx1024")));
            fail("expected starting java to fail");
        } catch (final RuntimeException e) {
            assertThat(e, hasToString(containsString(("starting java failed"))));
            assertThat(e, hasToString(containsString(("Too small maximum heap"))));
        }
    }

    public void testHeapSizeWithSpace() throws Exception {
        try {
            JvmOption.extractMaxHeapSize(JvmOption.findFinalOptions(Collections.singletonList("-Xmx 1024")));
            fail("expected starting java to fail");
        } catch (final RuntimeException e) {
            assertThat(e, hasToString(containsString(("starting java failed"))));
            assertThat(e, hasToString(containsString(("Invalid maximum heap size: -Xmx 1024"))));
        }
    }

    public void testMaxDirectMemorySizeUnset() throws Exception {
        assertThat(JvmOption.extractMaxDirectMemorySize(JvmOption.findFinalOptions(Collections.singletonList("-Xmx1g"))), equalTo(0L));
    }

    public void testMaxDirectMemorySizeSet() throws Exception {
        assertThat(
            JvmOption.extractMaxDirectMemorySize(JvmOption.findFinalOptions(Arrays.asList("-Xmx1g", "-XX:MaxDirectMemorySize=512m"))),
            equalTo(512L << 20)
        );
    }

    public void testExtractSystemProperties() {
        Map<String, String> expectedSystemProperties = new HashMap<>();
        expectedSystemProperties.put("file.encoding", "UTF-8");
        expectedSystemProperties.put("kv.setting", "ABC=DEF");

        Map<String, String> parsedSystemProperties = JvmErgonomics.extractSystemProperties(
            Arrays.asList("-Dfile.encoding=UTF-8", "-Dkv.setting=ABC=DEF")
        );

        assertEquals(expectedSystemProperties, parsedSystemProperties);
    }

    public void testG1GOptionsForSmallHeap() throws Exception {
        long heapSize = 6L << 30;
        Map<String, JvmOption> opts = buildG1Options(heapSize, Map.of());
        List<String> jvmErgonomics = JvmErgonomics.choose(opts, heapSize, Settings.EMPTY);
        assertThat(jvmErgonomics, hasItem("-XX:G1HeapRegionSize=4m"));
        assertThat(jvmErgonomics, hasItem("-XX:InitiatingHeapOccupancyPercent=30"));
        assertThat(jvmErgonomics, hasItem("-XX:G1ReservePercent=15"));
    }

    public void testG1GOptionsForSmallHeapWhenTuningSet() throws Exception {
        long heapSize = 6L << 30;
        Map<String, JvmOption> opts = buildG1Options(
            heapSize,
            Map.of(
                "G1HeapRegionSize",
                new JvmOption("4194304", "command line"),
                "InitiatingHeapOccupancyPercent",
                new JvmOption("45", "command line")
            )
        );
        List<String> jvmErgonomics = JvmErgonomics.choose(opts, heapSize, Settings.EMPTY);
        assertThat(jvmErgonomics, everyItem(not(startsWith("-XX:G1HeapRegionSize="))));
        assertThat(jvmErgonomics, everyItem(not(startsWith("-XX:InitiatingHeapOccupancyPercent="))));
        assertThat(jvmErgonomics, hasItem("-XX:G1ReservePercent=15"));
    }

    public void testG1GOptionsForLargeHeap() throws Exception {
        long heapSize = 8L << 30;
        Map<String, JvmOption> opts = buildG1Options(heapSize, Map.of());
        List<String> jvmErgonomics = JvmErgonomics.choose(opts, heapSize, Settings.EMPTY);
        assertThat(jvmErgonomics, hasItem("-XX:InitiatingHeapOccupancyPercent=30"));
        assertThat(jvmErgonomics, hasItem("-XX:G1ReservePercent=25"));
        assertThat(jvmErgonomics, everyItem(not(startsWith("-XX:G1HeapRegionSize="))));
    }

    public void testG1GOptionsForSmallHeapWhenOtherGCSet() throws Exception {
        long heapSize = 6L << 30;
        Map<String, JvmOption> opts = buildFinalOptions(heapSize, false, Map.of());
        List<String> jvmErgonomics = JvmErgonomics.choose(opts, heapSize, Settings.EMPTY);
        assertThat(jvmErgonomics, everyItem(not(startsWith("-XX:G1HeapRegionSize="))));
        assertThat(jvmErgonomics, everyItem(not(startsWith("-XX:InitiatingHeapOccupancyPercent="))));
        assertThat(jvmErgonomics, everyItem(not(startsWith("-XX:G1ReservePercent="))));
    }

    public void testG1GOptionsForLargeHeapWhenTuningSet() throws Exception {
        long heapSize = 8L << 30;
        Map<String, JvmOption> opts = buildG1Options(
            heapSize,
            Map.of(
                "InitiatingHeapOccupancyPercent",
                new JvmOption("60", "command line"),
                "G1ReservePercent",
                new JvmOption("10", "command line")
            )
        );
        List<String> jvmErgonomics = JvmErgonomics.choose(opts, heapSize, Settings.EMPTY);
        assertThat(jvmErgonomics, everyItem(not(startsWith("-XX:InitiatingHeapOccupancyPercent="))));
        assertThat(jvmErgonomics, everyItem(not(startsWith("-XX:G1ReservePercent="))));
        assertThat(jvmErgonomics, everyItem(not(startsWith("-XX:G1HeapRegionSize="))));
    }

    public void testExtractNoSystemProperties() {
        Map<String, String> parsedSystemProperties = JvmErgonomics.extractSystemProperties(Arrays.asList("-Xms1024M", "-Xmx1024M"));
        assertTrue(parsedSystemProperties.isEmpty());
    }

    public void testMaxDirectMemorySizeChoice() throws Exception {
        final Map<String, Long> heapSizes = Map.of(
            "64M",
            64L << 20,
            "512M",
            512L << 20,
            "1024M",
            1024L << 20,
            "1G",
            1L << 30,
            "2048M",
            2048L << 20,
            "2G",
            2L << 30,
            "8G",
            8L << 30
        );
        final String heapLabel = randomFrom(heapSizes.keySet().toArray(String[]::new));
        final long heapSize = heapSizes.get(heapLabel);
        Map<String, JvmOption> opts = buildG1Options(heapSize, Map.of());
        assertThat(
            JvmErgonomics.choose(opts, heapSize, Settings.EMPTY),
            hasItem("-XX:MaxDirectMemorySize=" + (long) (heapSize * JvmErgonomics.DIRECT_MEMORY_TO_HEAP_FACTOR))
        );
    }

    public void testMaxDirectMemorySizeChoiceWhenSet() throws Exception {
        long heapSize = 1L << 30;
        Map<String, JvmOption> opts = buildG1Options(
            heapSize,
            Map.of("MaxDirectMemorySize", new JvmOption(Long.toString(1L << 30), "command line"))
        );
        assertThat(JvmErgonomics.choose(opts, heapSize, Settings.EMPTY), everyItem(not(startsWith("-XX:MaxDirectMemorySize="))));
    }

    public void testConcGCThreadsNotSetBasedOnProcessors() throws Exception {
        Settings.Builder nodeSettingsBuilder = Settings.builder()
            .put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.SEARCH_ROLE.roleName());
        if (randomBoolean()) {
            int maxProcessors = Runtime.getRuntime().availableProcessors();
            List<Integer> possibleProcessors = new ArrayList<>();
            IntStream.range(1, maxProcessors + 1).filter(i -> i < 4 || i > 5).forEach(possibleProcessors::add);
            nodeSettingsBuilder.put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), randomFrom(possibleProcessors));
        }
        long defaultHeapSize = 1L << 30;
        Map<String, JvmOption> opts = buildG1Options(defaultHeapSize, Map.of());
        assertThat(
            JvmErgonomics.choose(opts, defaultHeapSize, nodeSettingsBuilder.build()),
            everyItem(not(startsWith("-XX:ConcGCThreads=")))
        );
    }

    public void testConcGCThreadsNotSetBasedOnRoles() throws Exception {
        Settings.Builder nodeSettingsBuilder = Settings.builder().put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), between(4, 5));
        if (randomBoolean()) {
            List<DiscoveryNodeRole> possibleRoles = new ArrayList<>(DiscoveryNodeRole.roles());
            possibleRoles.remove(DiscoveryNodeRole.SEARCH_ROLE);
            possibleRoles.remove(DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE);
            nodeSettingsBuilder.put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), randomFrom(possibleRoles).roleName());
        }
        long defaultHeapSize = 1L << 30;
        Map<String, JvmOption> opts = buildG1Options(defaultHeapSize, Map.of());
        assertThat(
            JvmErgonomics.choose(opts, defaultHeapSize, nodeSettingsBuilder.build()),
            everyItem(not(startsWith("-XX:ConcGCThreads=")))
        );
    }

    public void testConcGCThreadsSet() throws Exception {
        Settings nodeSettings = Settings.builder()
            .put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), between(4, 5))
            .put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.SEARCH_ROLE.roleName())
            .build();
        long defaultHeapSize = 1L << 30;
        Map<String, JvmOption> opts = buildG1Options(defaultHeapSize, Map.of());
        assertThat(JvmErgonomics.choose(opts, defaultHeapSize, nodeSettings), hasItem("-XX:ConcGCThreads=2"));
    }

    public void testMinimumNewSizeNotSetBasedOnHeap() throws Exception {
        Settings nodeSettings = Settings.builder()
            .put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.SEARCH_ROLE.roleName())
            .build();
        long heapSize = (long) between(5, 31) << 30;
        Map<String, JvmOption> opts = buildG1Options(heapSize, Map.of());
        List<String> chosen = JvmErgonomics.choose(opts, heapSize, nodeSettings);
        assertThat(chosen, everyItem(not(is("-XX:+UnlockExperimentalVMOptions"))));
        assertThat(chosen, everyItem(not(startsWith("-XX:G1NewSizePercent="))));
    }

    public void testMinimumNewSizeNotSetBasedOnRoles() throws Exception {
        Settings nodeSettings;
        if (randomBoolean()) {
            nodeSettings = Settings.EMPTY;
        } else {
            List<DiscoveryNodeRole> possibleRoles = new ArrayList<>(DiscoveryNodeRole.roles());
            possibleRoles.remove(DiscoveryNodeRole.SEARCH_ROLE);
            possibleRoles.remove(DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE);
            nodeSettings = Settings.builder()
                .put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), randomFrom(possibleRoles).roleName())
                .build();
        }
        long heapSize = (long) between(1, 4) << 30;
        Map<String, JvmOption> opts = buildG1Options(heapSize, Map.of());
        List<String> chosen = JvmErgonomics.choose(opts, heapSize, nodeSettings);
        assertThat(chosen, everyItem(not(is("-XX:+UnlockExperimentalVMOptions"))));
        assertThat(chosen, everyItem(not(startsWith("-XX:G1NewSizePercent="))));
    }

    public void testMinimumNewSizeSet() throws Exception {
        Settings nodeSettings = Settings.builder()
            .put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.SEARCH_ROLE.roleName())
            .build();
        long heapSize = (long) between(1, 4) << 30;
        Map<String, JvmOption> opts = buildG1Options(heapSize, Map.of());
        List<String> chosen = JvmErgonomics.choose(opts, heapSize, nodeSettings);
        assertThat(chosen, hasItem("-XX:+UnlockExperimentalVMOptions"));
        assertThat(chosen, hasItem("-XX:G1NewSizePercent=10"));
    }

    @SuppressWarnings("ConstantConditions")
    public void testMissingOptionHandling() {
        final Map<String, JvmOption> g1GcOn = Map.of("UseG1GC", new JvmOption("true", ""));
        final Map<String, JvmOption> g1GcOff = Map.of("UseG1GC", new JvmOption("", ""));

        assertFalse(JvmErgonomics.tuneG1GCHeapRegion(Map.of(), false));
        assertThat(
            expectThrows(IllegalStateException.class, () -> JvmErgonomics.tuneG1GCHeapRegion(Map.of(), true)).getMessage(),
            allOf(containsString("[UseG1GC]"), containsString("unexpectedly missing"))
        );
        assertThat(
            expectThrows(IllegalStateException.class, () -> JvmErgonomics.tuneG1GCHeapRegion(g1GcOn, true)).getMessage(),
            allOf(containsString("[G1HeapRegionSize]"), containsString("unexpectedly missing"))
        );
        assertFalse(JvmErgonomics.tuneG1GCHeapRegion(g1GcOff, randomBoolean()));

        assertThat(
            expectThrows(IllegalStateException.class, () -> JvmErgonomics.tuneG1GCReservePercent(Map.of(), randomBoolean())).getMessage(),
            allOf(containsString("[UseG1GC]"), containsString("unexpectedly missing"))
        );
        assertThat(
            expectThrows(IllegalStateException.class, () -> JvmErgonomics.tuneG1GCReservePercent(g1GcOn, randomBoolean())).getMessage(),
            allOf(containsString("[G1ReservePercent]"), containsString("unexpectedly missing"))
        );
        assertEquals(0, JvmErgonomics.tuneG1GCReservePercent(g1GcOff, randomBoolean()));

        assertThat(
            expectThrows(IllegalStateException.class, () -> JvmErgonomics.tuneG1GCInitiatingHeapOccupancyPercent(Map.of())).getMessage(),
            allOf(containsString("[UseG1GC]"), containsString("unexpectedly missing"))
        );
        assertThat(
            expectThrows(IllegalStateException.class, () -> JvmErgonomics.tuneG1GCInitiatingHeapOccupancyPercent(g1GcOn)).getMessage(),
            allOf(containsString("[InitiatingHeapOccupancyPercent]"), containsString("unexpectedly missing"))
        );
        assertFalse(JvmErgonomics.tuneG1GCInitiatingHeapOccupancyPercent(g1GcOff));

        assertThat(
            expectThrows(IllegalStateException.class, () -> new JvmOption("OptionName", null)).getMessage(),
            allOf(containsString("could not determine the origin of JVM option [OptionName]"), containsString("unsupported"))
        );
    }

    /**
     * Builds a final JVM options map simulating G1GC being enabled with the given heap size.
     */
    private static Map<String, JvmOption> buildG1Options(long heapSize, Map<String, JvmOption> overrides) {
        return buildFinalOptions(heapSize, true, overrides);
    }

    /**
     * Builds a final JVM options map with the given settings.
     */
    private static Map<String, JvmOption> buildFinalOptions(long heapSize, boolean g1gc, Map<String, JvmOption> overrides) {
        Map<String, JvmOption> options = new HashMap<>();
        options.put("MaxHeapSize", new JvmOption(Long.toString(heapSize), "command line"));
        options.put("MaxDirectMemorySize", new JvmOption("0", "default"));
        options.put("UseG1GC", new JvmOption(g1gc ? "true" : "false", g1gc ? "default" : "command line"));
        options.put("G1HeapRegionSize", new JvmOption("0", "default"));
        options.put("InitiatingHeapOccupancyPercent", new JvmOption("45", "default"));
        options.put("G1ReservePercent", new JvmOption("10", "default"));
        options.putAll(overrides);
        return options;
    }

}
