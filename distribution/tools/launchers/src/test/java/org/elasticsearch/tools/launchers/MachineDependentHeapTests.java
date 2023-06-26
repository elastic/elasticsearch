/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tools.launchers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;

public class MachineDependentHeapTests extends LaunchersTestCase {

    public void testDefaultHeapSize() throws Exception {
        MachineDependentHeap heap = new MachineDependentHeap(systemMemoryInGigabytes(8));
        List<String> options = heap.determineHeapSettings(configPath(), Collections.emptyList());
        assertThat(options, containsInAnyOrder("-Xmx4096m", "-Xms4096m"));
    }

    public void testUserPassedHeapArgs() throws Exception {
        MachineDependentHeap heap = new MachineDependentHeap(systemMemoryInGigabytes(8));
        List<String> options = heap.determineHeapSettings(configPath(), Collections.singletonList("-Xmx4g"));
        assertThat(options, empty());

        options = heap.determineHeapSettings(configPath(), Collections.singletonList("-Xms4g"));
        assertThat(options, empty());

        options = heap.determineHeapSettings(configPath(), Collections.singletonList("-XX:MaxHeapSize=4g"));
        assertThat(options, empty());

        options = heap.determineHeapSettings(configPath(), Collections.singletonList("-XX:MinHeapSize=4g"));
        assertThat(options, empty());

        options = heap.determineHeapSettings(configPath(), Collections.singletonList("-XX:InitialHeapSize=4g"));
        assertThat(options, empty());
    }

    // Explicitly test odd heap sizes
    // See: https://github.com/elastic/elasticsearch/issues/86431
    public void testOddUserPassedHeapArgs() throws Exception {
        MachineDependentHeap heap = new MachineDependentHeap(systemMemoryInGigabytes(8));
        List<String> options = heap.determineHeapSettings(configPath(), Collections.singletonList("-Xmx409m"));
        assertThat(options, empty());

        options = heap.determineHeapSettings(configPath(), Collections.singletonList("-Xms409m"));
        assertThat(options, empty());
    }

    public void testMasterOnlyOptions() {
        List<String> options = calculateHeap(16, "master");
        assertThat(options, containsInAnyOrder("-Xmx9830m", "-Xms9830m"));

        options = calculateHeap(64, "master");
        assertThat(options, containsInAnyOrder("-Xmx31744m", "-Xms31744m"));
    }

    public void testMlOnlyOptions() {
        List<String> options = calculateHeap(1, "ml");
        assertThat(options, containsInAnyOrder("-Xmx409m", "-Xms409m"));

        options = calculateHeap(4, "ml");
        assertThat(options, containsInAnyOrder("-Xmx1024m", "-Xms1024m"));

        options = calculateHeap(32, "ml");
        assertThat(options, containsInAnyOrder("-Xmx2048m", "-Xms2048m"));
    }

    public void testDataNodeOptions() {
        List<String> options = calculateHeap(1, "data");
        assertThat(options, containsInAnyOrder("-Xmx512m", "-Xms512m"));

        options = calculateHeap(8, "data");
        assertThat(options, containsInAnyOrder("-Xmx4096m", "-Xms4096m"));

        options = calculateHeap(64, "data");
        assertThat(options, containsInAnyOrder("-Xmx31744m", "-Xms31744m"));

        options = calculateHeap(0.5, "data");
        assertThat(options, containsInAnyOrder("-Xmx204m", "-Xms204m"));

        options = calculateHeap(0.2, "data");
        assertThat(options, containsInAnyOrder("-Xmx128m", "-Xms128m"));
    }

    public void testFallbackOptions() throws Exception {
        MachineDependentHeap machineDependentHeap = new MachineDependentHeap(errorThrowingMemoryInfo());
        List<String> options = machineDependentHeap.determineHeapSettings(configPath(), Collections.emptyList());
        assertThat(options, containsInAnyOrder("-Xmx1024m", "-Xms1024m"));
    }

    private static List<String> calculateHeap(double memoryInGigabytes, String... roles) {
        MachineDependentHeap machineDependentHeap = new MachineDependentHeap(systemMemoryInGigabytes(memoryInGigabytes));
        String configYaml = "node.roles: [" + String.join(",", roles) + "]";
        return calculateHeap(machineDependentHeap, configYaml);
    }

    private static List<String> calculateHeap(MachineDependentHeap machineDependentHeap, String configYaml) {
        try (InputStream in = new ByteArrayInputStream(configYaml.getBytes(StandardCharsets.UTF_8))) {
            return machineDependentHeap.determineHeapSettings(in);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static SystemMemoryInfo systemMemoryInGigabytes(double gigabytes) {
        return () -> (long) (gigabytes * 1024 * 1024 * 1024);
    }

    private static SystemMemoryInfo errorThrowingMemoryInfo() {
        return () -> { throw new SystemMemoryInfo.SystemMemoryInfoException("something went wrong"); };
    }

    private static Path configPath() {
        URL resource = MachineDependentHeapTests.class.getResource("/config/elasticsearch.yml");
        try {
            return Paths.get(resource.toURI()).getParent();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
