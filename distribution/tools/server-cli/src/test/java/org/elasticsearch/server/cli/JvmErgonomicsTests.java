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
        List<String> jvmErgonomics = JvmErgonomics.choose(Arrays.asList("-Xms6g", "-Xmx6g", "-XX:+UseG1GC"), Settings.EMPTY);
        assertThat(jvmErgonomics, hasItem("-XX:G1HeapRegionSize=4m"));
        assertThat(jvmErgonomics, hasItem("-XX:InitiatingHeapOccupancyPercent=30"));
        assertThat(jvmErgonomics, hasItem("-XX:G1ReservePercent=15"));
    }

    public void testG1GOptionsForSmallHeapWhenTuningSet() throws Exception {
        List<String> jvmErgonomics = JvmErgonomics.choose(
            Arrays.asList("-Xms6g", "-Xmx6g", "-XX:+UseG1GC", "-XX:G1HeapRegionSize=4m", "-XX:InitiatingHeapOccupancyPercent=45"),
            Settings.EMPTY
        );
        assertThat(jvmErgonomics, everyItem(not(startsWith("-XX:G1HeapRegionSize="))));
        assertThat(jvmErgonomics, everyItem(not(startsWith("-XX:InitiatingHeapOccupancyPercent="))));
        assertThat(jvmErgonomics, hasItem("-XX:G1ReservePercent=15"));
    }

    public void testG1GOptionsForLargeHeap() throws Exception {
        List<String> jvmErgonomics = JvmErgonomics.choose(Arrays.asList("-Xms8g", "-Xmx8g", "-XX:+UseG1GC"), Settings.EMPTY);
        assertThat(jvmErgonomics, hasItem("-XX:InitiatingHeapOccupancyPercent=30"));
        assertThat(jvmErgonomics, hasItem("-XX:G1ReservePercent=25"));
        assertThat(jvmErgonomics, everyItem(not(startsWith("-XX:G1HeapRegionSize="))));
    }

    public void testG1GOptionsForSmallHeapWhenOtherGCSet() throws Exception {
        List<String> jvmErgonomics = JvmErgonomics.choose(Arrays.asList("-Xms6g", "-Xmx6g", "-XX:+UseParallelGC"), Settings.EMPTY);
        assertThat(jvmErgonomics, everyItem(not(startsWith("-XX:G1HeapRegionSize="))));
        assertThat(jvmErgonomics, everyItem(not(startsWith("-XX:InitiatingHeapOccupancyPercent="))));
        assertThat(jvmErgonomics, everyItem(not(startsWith("-XX:G1ReservePercent="))));
    }

    public void testG1GOptionsForLargeHeapWhenTuningSet() throws Exception {
        List<String> jvmErgonomics = JvmErgonomics.choose(
            Arrays.asList("-Xms8g", "-Xmx8g", "-XX:+UseG1GC", "-XX:InitiatingHeapOccupancyPercent=60", "-XX:G1ReservePercent=10"),
            Settings.EMPTY
        );
        assertThat(jvmErgonomics, everyItem(not(startsWith("-XX:InitiatingHeapOccupancyPercent="))));
        assertThat(jvmErgonomics, everyItem(not(startsWith("-XX:G1ReservePercent="))));
        assertThat(jvmErgonomics, everyItem(not(startsWith("-XX:G1HeapRegionSize="))));
    }

    public void testExtractNoSystemProperties() {
        Map<String, String> parsedSystemProperties = JvmErgonomics.extractSystemProperties(Arrays.asList("-Xms1024M", "-Xmx1024M"));
        assertTrue(parsedSystemProperties.isEmpty());
    }

    public void testMaxDirectMemorySizeChoice() throws Exception {
        final Map<String, String> heapMaxDirectMemorySize = Map.of(
            "64M",
            Long.toString((64L << 20) / 2),
            "512M",
            Long.toString((512L << 20) / 2),
            "1024M",
            Long.toString((1024L << 20) / 2),
            "1G",
            Long.toString((1L << 30) / 2),
            "2048M",
            Long.toString((2048L << 20) / 2),
            "2G",
            Long.toString((2L << 30) / 2),
            "8G",
            Long.toString((8L << 30) / 2)
        );
        final String heapSize = randomFrom(heapMaxDirectMemorySize.keySet().toArray(String[]::new));
        assertThat(
            JvmErgonomics.choose(Arrays.asList("-Xms" + heapSize, "-Xmx" + heapSize), Settings.EMPTY),
            hasItem("-XX:MaxDirectMemorySize=" + heapMaxDirectMemorySize.get(heapSize))
        );
    }

    public void testMaxDirectMemorySizeChoiceWhenSet() throws Exception {
        assertThat(
            JvmErgonomics.choose(Arrays.asList("-Xms1g", "-Xmx1g", "-XX:MaxDirectMemorySize=1g"), Settings.EMPTY),
            everyItem(not(startsWith("-XX:MaxDirectMemorySize=")))
        );
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
        assertThat(JvmErgonomics.choose(List.of(), nodeSettingsBuilder.build()), everyItem(not(startsWith("-XX:ConcGCThreads="))));
    }

    public void testConcGCThreadsNotSetBasedOnRoles() throws Exception {
        Settings.Builder nodeSettingsBuilder = Settings.builder().put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), between(4, 5));
        if (randomBoolean()) {
            List<DiscoveryNodeRole> possibleRoles = new ArrayList<>(DiscoveryNodeRole.roles());
            possibleRoles.remove(DiscoveryNodeRole.SEARCH_ROLE);
            possibleRoles.remove(DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE);
            nodeSettingsBuilder.put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), randomFrom(possibleRoles).roleName());
        }
        assertThat(JvmErgonomics.choose(List.of(), nodeSettingsBuilder.build()), everyItem(not(startsWith("-XX:ConcGCThreads="))));

    }

    public void testConcGCThreadsSet() throws Exception {
        Settings nodeSettings = Settings.builder()
            .put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), between(4, 5))
            .put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.SEARCH_ROLE.roleName())
            .build();
        assertThat(JvmErgonomics.choose(List.of(), nodeSettings), hasItem("-XX:ConcGCThreads=2"));
    }

    public void testMinimumNewSizeNotSetBasedOnHeap() throws Exception {
        Settings nodeSettings = Settings.builder()
            .put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.SEARCH_ROLE.roleName())
            .build();
        List<String> chosen = JvmErgonomics.choose(List.of("-Xmx" + between(5, 31) + "g"), nodeSettings);
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
        List<String> chosen = JvmErgonomics.choose(List.of("-Xmx" + between(1, 4) + "g"), nodeSettings);
        assertThat(chosen, everyItem(not(is("-XX:+UnlockExperimentalVMOptions"))));
        assertThat(chosen, everyItem(not(startsWith("-XX:G1NewSizePercent="))));
    }

    public void testMinimumNewSizeSet() throws Exception {
        Settings nodeSettings = Settings.builder()
            .put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.SEARCH_ROLE.roleName())
            .build();
        List<String> chosen = JvmErgonomics.choose(List.of("-Xmx" + between(1, 4) + "g"), nodeSettings);
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

}
