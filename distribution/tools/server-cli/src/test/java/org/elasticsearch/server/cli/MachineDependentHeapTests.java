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
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

// TODO: rework these tests to mock jvm option finder so they can run with security manager, no forking needed
public class MachineDependentHeapTests extends ESTestCase {

    private static final long SERVER_CLI_OVERHEAD = 120 * 1024L * 1024L;

    public void testDefaultHeapSize() throws Exception {
        MachineDependentHeap heap = new MachineDependentHeap();
        List<String> options = heap.determineHeapSettings(Settings.EMPTY, systemMemoryInGigabytes(8), Collections.emptyList());
        assertThat(options, containsInAnyOrder("-Xmx4036m", "-Xms4036m"));
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
        assertHeapOptions(16, containsInAnyOrder("-Xmx9758m", "-Xms9758m"), "master");
        assertHeapOptions(64, containsInAnyOrder("-Xmx31744m", "-Xms31744m"), "master");
    }

    public void testMlOnlyOptions_new() throws Exception {
        assumeTrue("feature flag must be enabled for new memory computation", new FeatureFlag("new_ml_memory_computation").isEnabled());
        assertHeapOptions(1, containsInAnyOrder("-Xmx240m", "-Xms240m"), "ml");
        assertHeapOptions(4, containsInAnyOrder("-Xmx1060m", "-Xms1060m"), "ml");
        assertHeapOptions(32, containsInAnyOrder("-Xmx5452m", "-Xms5452m"), "ml");
        assertHeapOptions(64, containsInAnyOrder("-Xmx7636m", "-Xms7636m"), "ml");
        // We'd never see a node this big in Cloud, but this assertion proves that the 31GB absolute maximum
        // eventually kicks in (because 0.4 * 16 + 0.1 * (263 - 16) > 31)
        assertHeapOptions(263, containsInAnyOrder("-Xmx21220m", "-Xms21220m"), "ml");
    }

    public void testMlOnlyOptions_old() throws Exception {
        assumeTrue(
            "feature flag must be disabled for old memory computation",
            new FeatureFlag("new_ml_memory_computation").isEnabled() == false
        );
        assertHeapOptions(1, containsInAnyOrder("-Xmx360m", "-Xms360m"), "ml");
        assertHeapOptions(4, containsInAnyOrder("-Xmx1588m", "-Xms1588m"), "ml");
        assertHeapOptions(32, containsInAnyOrder("-Xmx8176m", "-Xms8176m"), "ml");
        assertHeapOptions(64, containsInAnyOrder("-Xmx11452m", "-Xms11452m"), "ml");
        // We'd never see a node this big in Cloud, but this assertion proves that the 31GB absolute maximum
        // eventually kicks in (because 0.4 * 16 + 0.1 * (263 - 16) > 31)
        assertHeapOptions(263, containsInAnyOrder("-Xmx31744m", "-Xms31744m"), "ml");
    }

    public void testDataNodeOptions() throws Exception {
        double oneGbPlusOverhead = 1.0 + SERVER_CLI_OVERHEAD / (double) (1024L * 1024L * 1024L);
        assertHeapOptions(oneGbPlusOverhead, containsInAnyOrder("-Xmx512m", "-Xms512m"), "data");
        assertHeapOptions(8, containsInAnyOrder("-Xmx4036m", "-Xms4036m"), "data");
        assertHeapOptions(64, containsInAnyOrder("-Xmx31744m", "-Xms31744m"), "data");
        assertHeapOptions(0.5, containsInAnyOrder("-Xmx156m", "-Xms156m"), "data");
        assertHeapOptions(0.2, containsInAnyOrder("-Xmx128m", "-Xms128m"), "data");
    }

    public void testMemoryLessThanOverhead() throws Exception {
        double lessThanOverhead = 0.05; // ~50 MB
        assertHeapOptions(lessThanOverhead, containsInAnyOrder("-Xmx128m", "-Xms128m"), "data");
        assertHeapOptions(lessThanOverhead, containsInAnyOrder("-Xmx32m", "-Xms32m"), "ml");
        assertHeapOptions(lessThanOverhead, containsInAnyOrder("-Xmx76m", "-Xms76m"), "master");
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
