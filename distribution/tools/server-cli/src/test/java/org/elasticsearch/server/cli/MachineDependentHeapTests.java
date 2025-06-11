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
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

// TODO: rework these tests to mock jvm option finder so they can run with security manager, no forking needed
public class MachineDependentHeapTests extends ESTestCase {

    public void testDefaultHeapSize() throws Exception {
        MachineDependentHeap heap = new MachineDependentHeap();
        List<String> options = heap.determineHeapSettings(Settings.EMPTY, systemMemoryInGigabytes(8), Collections.emptyList());
        assertThat(options, containsInAnyOrder("-Xmx4096m", "-Xms4096m"));
    }

    public void testUserPassedHeapArgs() throws Exception {
        var systemMemoryInfo = systemMemoryInGigabytes(8);
        MachineDependentHeap heap = new MachineDependentHeap();
        List<String> options = heap.determineHeapSettings(Settings.EMPTY, systemMemoryInfo, List.of("-Xmx4g"));
        assertThat(options, empty());

        options = heap.determineHeapSettings(Settings.EMPTY, systemMemoryInfo, List.of("-Xms4g"));
        assertThat(options, empty());
    }

    // Explicitly test odd heap sizes
    // See: https://github.com/elastic/elasticsearch/issues/86431
    public void testOddUserPassedHeapArgs() throws Exception {
        var systemMemoryInfo = systemMemoryInGigabytes(8);
        MachineDependentHeap heap = new MachineDependentHeap();
        List<String> options = heap.determineHeapSettings(Settings.EMPTY, systemMemoryInfo, List.of("-Xmx409m"));
        assertThat(options, empty());

        options = heap.determineHeapSettings(Settings.EMPTY, systemMemoryInfo, List.of("-Xms409m"));
        assertThat(options, empty());
    }

    public void testMasterOnlyOptions() throws Exception {
        assertHeapOptions(16, containsInAnyOrder("-Xmx9830m", "-Xms9830m"), "master");
        assertHeapOptions(64, containsInAnyOrder("-Xmx31744m", "-Xms31744m"), "master");
    }

    public void testMlOnlyOptions() throws Exception {
        assertHeapOptions(1, containsInAnyOrder("-Xmx272m", "-Xms272m"), "ml");
        assertHeapOptions(4, containsInAnyOrder("-Xmx1092m", "-Xms1092m"), "ml");
        assertHeapOptions(32, containsInAnyOrder("-Xmx5460m", "-Xms5460m"), "ml");
        assertHeapOptions(64, containsInAnyOrder("-Xmx7644m", "-Xms7644m"), "ml");
        // We'd never see a node this big in Cloud, but this assertion proves that the 31GB absolute maximum
        // eventually kicks in (because 0.4 * 16 + 0.1 * (263 - 16) > 31)
        assertHeapOptions(263, containsInAnyOrder("-Xmx21228m", "-Xms21228m"), "ml");
    }

    public void testDataNodeOptions() throws Exception {
        assertHeapOptions(1, containsInAnyOrder("-Xmx512m", "-Xms512m"), "data");
        assertHeapOptions(8, containsInAnyOrder("-Xmx4096m", "-Xms4096m"), "data");
        assertHeapOptions(64, containsInAnyOrder("-Xmx31744m", "-Xms31744m"), "data");
        assertHeapOptions(0.5, containsInAnyOrder("-Xmx204m", "-Xms204m"), "data");
        assertHeapOptions(0.2, containsInAnyOrder("-Xmx128m", "-Xms128m"), "data");
    }

    private void assertHeapOptions(double memoryInGigabytes, Matcher<Iterable<? extends String>> optionsMatcher, String... roles)
        throws Exception {
        SystemMemoryInfo systemMemoryInfo = systemMemoryInGigabytes(memoryInGigabytes);
        MachineDependentHeap machineDependentHeap = new MachineDependentHeap();
        Settings nodeSettings = Settings.builder().putList("node.roles", roles).build();
        List<String> heapOptions = machineDependentHeap.determineHeapSettings(nodeSettings, systemMemoryInfo, Collections.emptyList());
        assertThat(heapOptions, optionsMatcher);
    }

    private static SystemMemoryInfo systemMemoryInGigabytes(double gigabytes) {
        return () -> (long) (gigabytes * 1024 * 1024 * 1024);
    }
}
