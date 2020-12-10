/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
        List<String> options = heap.determineHeapSettings(configPath(), List.of("-Xmx4g"));
        assertThat(options, empty());

        options = heap.determineHeapSettings(configPath(), List.of("-Xms4g"));
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
