/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.server.cli;

import org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTestCase.WithoutSecurityManager;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@WithoutSecurityManager
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
        List<String> jvmErgonomics = JvmErgonomics.choose(Arrays.asList("-Xms6g", "-Xmx6g", "-XX:+UseG1GC"));
        assertThat(jvmErgonomics, hasItem("-XX:G1HeapRegionSize=4m"));
        assertThat(jvmErgonomics, hasItem("-XX:InitiatingHeapOccupancyPercent=30"));
        assertThat(jvmErgonomics, hasItem("-XX:G1ReservePercent=15"));
    }

    public void testG1GOptionsForSmallHeapWhenTuningSet() throws Exception {
        List<String> jvmErgonomics = JvmErgonomics.choose(
            Arrays.asList("-Xms6g", "-Xmx6g", "-XX:+UseG1GC", "-XX:G1HeapRegionSize=4m", "-XX:InitiatingHeapOccupancyPercent=45")
        );
        assertThat(jvmErgonomics, everyItem(not(startsWith("-XX:G1HeapRegionSize="))));
        assertThat(jvmErgonomics, everyItem(not(startsWith("-XX:InitiatingHeapOccupancyPercent="))));
        assertThat(jvmErgonomics, hasItem("-XX:G1ReservePercent=15"));
    }

    public void testG1GOptionsForLargeHeap() throws Exception {
        List<String> jvmErgonomics = JvmErgonomics.choose(Arrays.asList("-Xms8g", "-Xmx8g", "-XX:+UseG1GC"));
        assertThat(jvmErgonomics, hasItem("-XX:InitiatingHeapOccupancyPercent=30"));
        assertThat(jvmErgonomics, hasItem("-XX:G1ReservePercent=25"));
        assertThat(jvmErgonomics, everyItem(not(startsWith("-XX:G1HeapRegionSize="))));
    }

    public void testG1GOptionsForSmallHeapWhenOtherGCSet() throws Exception {
        List<String> jvmErgonomics = JvmErgonomics.choose(Arrays.asList("-Xms6g", "-Xmx6g", "-XX:+UseParallelGC"));
        assertThat(jvmErgonomics, everyItem(not(startsWith("-XX:G1HeapRegionSize="))));
        assertThat(jvmErgonomics, everyItem(not(startsWith("-XX:InitiatingHeapOccupancyPercent="))));
        assertThat(jvmErgonomics, everyItem(not(startsWith("-XX:G1ReservePercent="))));
    }

    public void testG1GOptionsForLargeHeapWhenTuningSet() throws Exception {
        List<String> jvmErgonomics = JvmErgonomics.choose(
            Arrays.asList("-Xms8g", "-Xmx8g", "-XX:+UseG1GC", "-XX:InitiatingHeapOccupancyPercent=60", "-XX:G1ReservePercent=10")
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
            JvmErgonomics.choose(Arrays.asList("-Xms" + heapSize, "-Xmx" + heapSize)),
            hasItem("-XX:MaxDirectMemorySize=" + heapMaxDirectMemorySize.get(heapSize))
        );
    }

    public void testMaxDirectMemorySizeChoiceWhenSet() throws Exception {
        assertThat(
            JvmErgonomics.choose(Arrays.asList("-Xms1g", "-Xmx1g", "-XX:MaxDirectMemorySize=1g")),
            everyItem(not(startsWith("-XX:MaxDirectMemorySize=")))
        );
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
