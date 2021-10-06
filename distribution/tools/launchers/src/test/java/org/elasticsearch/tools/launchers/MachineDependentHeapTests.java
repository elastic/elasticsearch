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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class MachineDependentHeapTests extends LaunchersTestCase {

    private static final long BYTES_IN_GB = 1024L * 1024L * 1024L;

    public void testDefaultHeapSize() throws Exception {
        MachineDependentHeap heap = new MachineDependentHeap(systemMemoryInGigabytes(8));
        List<String> options = heap.determineHeapSettings(configPath(), Collections.emptyList());
        assertThat(options, containsInAnyOrder("-Xmx4096m", "-Xms4096m"));
    }

    public void testUserPassedHeapArgs() throws Exception {
        MachineDependentHeap heap = new MachineDependentHeap(systemMemoryInGigabytes(8));
        List<String> options = heap.determineHeapSettings(configPath(), List.of("-Xmx4g"));
        assertThat(options, empty());

        options = heap.determineHeapSettings(configPath(), List.of("-Xms4g"));
        assertThat(options, empty());
    }

    public void testMasterOnlyOptions() {
        List<String> options = calculateHeap(16, null, "master");
        assertThat(options, containsInAnyOrder("-Xmx9830m", "-Xms9830m"));

        calculateHeap(20, 16 * BYTES_IN_GB, "master");
        assertThat(options, containsInAnyOrder("-Xmx9830m", "-Xms9830m"));

        options = calculateHeap(64, null, "master");
        assertThat(options, containsInAnyOrder("-Xmx31744m", "-Xms31744m"));

        options = calculateHeap(77, 64 * BYTES_IN_GB, "master");
        assertThat(options, containsInAnyOrder("-Xmx31744m", "-Xms31744m"));
    }

    public void testMlOnlyOptions() {
        List<String> options = calculateHeap(1, null, "ml");
        assertThat(options, containsInAnyOrder("-Xmx409m", "-Xms409m"));

        options = calculateHeap(4, BYTES_IN_GB, "ml");
        assertThat(options, containsInAnyOrder("-Xmx409m", "-Xms409m"));

        options = calculateHeap(4, null, "ml");
        assertThat(options, containsInAnyOrder("-Xmx1024m", "-Xms1024m"));

        options = calculateHeap(6, 4 * BYTES_IN_GB, "ml");
        assertThat(options, containsInAnyOrder("-Xmx1024m", "-Xms1024m"));

        options = calculateHeap(32, null, "ml");
        assertThat(options, containsInAnyOrder("-Xmx2048m", "-Xms2048m"));

        options = calculateHeap(100, 32 * BYTES_IN_GB, "ml");
        assertThat(options, containsInAnyOrder("-Xmx2048m", "-Xms2048m"));
    }

    public void testDataNodeOptions() {
        List<String> options = calculateHeap(1, null, "data");
        assertThat(options, containsInAnyOrder("-Xmx512m", "-Xms512m"));

        options = calculateHeap(5, BYTES_IN_GB, "data");
        assertThat(options, containsInAnyOrder("-Xmx512m", "-Xms512m"));

        options = calculateHeap(8, null, "data");
        assertThat(options, containsInAnyOrder("-Xmx4096m", "-Xms4096m"));

        options = calculateHeap(42, 8 * BYTES_IN_GB, "data");
        assertThat(options, containsInAnyOrder("-Xmx4096m", "-Xms4096m"));

        options = calculateHeap(64, null, "data");
        assertThat(options, containsInAnyOrder("-Xmx31744m", "-Xms31744m"));

        options = calculateHeap(65, 64 * BYTES_IN_GB, "data");
        assertThat(options, containsInAnyOrder("-Xmx31744m", "-Xms31744m"));

        options = calculateHeap(0.5, null, "data");
        assertThat(options, containsInAnyOrder("-Xmx204m", "-Xms204m"));

        options = calculateHeap(3, BYTES_IN_GB / 2, "data");
        assertThat(options, containsInAnyOrder("-Xmx204m", "-Xms204m"));

        options = calculateHeap(0.2, null, "data");
        assertThat(options, containsInAnyOrder("-Xmx128m", "-Xms128m"));

        options = calculateHeap(1, BYTES_IN_GB / 5, "data");
        assertThat(options, containsInAnyOrder("-Xmx128m", "-Xms128m"));
    }

    public void testFallbackOptions() throws Exception {
        MachineDependentHeap machineDependentHeap = new MachineDependentHeap(errorThrowingMemoryInfo());
        List<String> options = machineDependentHeap.determineHeapSettings(configPath(), Collections.emptyList());
        assertThat(options, containsInAnyOrder("-Xmx1024m", "-Xms1024m"));
    }

    public void testParseForcedMemoryInBytes() {
        assertThat(MachineDependentHeap.parseForcedMemoryInBytes(List.of("-Da=b", "-Dx=y")), nullValue());
        assertThat(
            MachineDependentHeap.parseForcedMemoryInBytes(List.of("-Da=b", "-Des.total_memory_bytes=123456789", "-Dx=y")),
            is(123456789L)
        );
        assertThat(MachineDependentHeap.parseForcedMemoryInBytes(List.of("-Des.total_memory_bytes=987654321")), is(987654321L));
        try {
            MachineDependentHeap.parseForcedMemoryInBytes(List.of("-Des.total_memory_bytes=invalid"));
            fail("expected parse to fail");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("Unable to parse number of bytes from [-Des.total_memory_bytes=invalid]"));
        }
    }

    private static List<String> calculateHeap(double memoryInGigabytes, Long forcedMemoryInBytes, String... roles) {
        MachineDependentHeap machineDependentHeap = new MachineDependentHeap(systemMemoryInGigabytes(memoryInGigabytes));
        String configYaml = "node.roles: [" + String.join(",", roles) + "]";
        return calculateHeap(machineDependentHeap, configYaml, forcedMemoryInBytes);
    }

    private static List<String> calculateHeap(MachineDependentHeap machineDependentHeap, String configYaml, Long forcedMemoryInBytes) {
        try (InputStream in = new ByteArrayInputStream(configYaml.getBytes(StandardCharsets.UTF_8))) {
            return machineDependentHeap.determineHeapSettings(in, forcedMemoryInBytes);
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
