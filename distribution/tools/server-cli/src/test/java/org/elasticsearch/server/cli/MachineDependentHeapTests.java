/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTestCase.WithoutSecurityManager;

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

// TODO: rework these tests to mock jvm option finder so they can run with security manager, no forking needed
@WithoutSecurityManager
public class MachineDependentHeapTests extends ESTestCase {

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

    // Explicitly test odd heap sizes
    // See: https://github.com/elastic/elasticsearch/issues/86431
    public void testOddUserPassedHeapArgs() throws Exception {
        MachineDependentHeap heap = new MachineDependentHeap(systemMemoryInGigabytes(8));
        List<String> options = heap.determineHeapSettings(configPath(), List.of("-Xmx409m"));
        assertThat(options, empty());

        options = heap.determineHeapSettings(configPath(), List.of("-Xms409m"));
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
        assertThat(options, containsInAnyOrder("-Xmx408m", "-Xms408m"));

        options = calculateHeap(4, "ml");
        assertThat(options, containsInAnyOrder("-Xmx1636m", "-Xms1636m"));

        options = calculateHeap(32, "ml");
        assertThat(options, containsInAnyOrder("-Xmx8192m", "-Xms8192m"));

        options = calculateHeap(64, "ml");
        assertThat(options, containsInAnyOrder("-Xmx11468m", "-Xms11468m"));

        // We'd never see a node this big in Cloud, but this assertion proves that the 31GB absolute maximum
        // eventually kicks in (because 0.4 * 16 + 0.1 * (263 - 16) > 31)
        options = calculateHeap(263, "ml");
        assertThat(options, containsInAnyOrder("-Xmx31744m", "-Xms31744m"));

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

    private static Path configPath() {
        URL resource = MachineDependentHeapTests.class.getResource("/config/elasticsearch.yml");
        try {
            return Paths.get(resource.toURI()).getParent();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
