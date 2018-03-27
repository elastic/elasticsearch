/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.test.rest.yaml;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.ESTestCase;

import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class ESClientYamlSuiteTestCaseTests extends ESTestCase {

    public void testLoadAllYamlSuites() throws Exception {
        Map<String,Set<Path>> yamlSuites = ESClientYamlSuiteTestCase.loadSuites("");
        assertEquals(2, yamlSuites.size());
    }

    public void testLoadSingleYamlSuite() throws Exception {
        Map<String,Set<Path>> yamlSuites = ESClientYamlSuiteTestCase.loadSuites("suite1/10_basic");
        assertSingleFile(yamlSuites, "suite1", "10_basic.yml");

        //extension .yaml is optional
        yamlSuites = ESClientYamlSuiteTestCase.loadSuites("suite1/10_basic");
        assertSingleFile(yamlSuites, "suite1", "10_basic.yml");
    }

    public void testLoadMultipleYamlSuites() throws Exception {
        //single directory
        Map<String,Set<Path>> yamlSuites = ESClientYamlSuiteTestCase.loadSuites("suite1");
        assertThat(yamlSuites, notNullValue());
        assertThat(yamlSuites.size(), equalTo(1));
        assertThat(yamlSuites.containsKey("suite1"), equalTo(true));
        assertThat(yamlSuites.get("suite1").size(), greaterThan(1));

        //multiple directories
        yamlSuites = ESClientYamlSuiteTestCase.loadSuites("suite1", "suite2");
        assertThat(yamlSuites, notNullValue());
        assertThat(yamlSuites.size(), equalTo(2));
        assertThat(yamlSuites.containsKey("suite1"), equalTo(true));
        assertEquals(2, yamlSuites.get("suite1").size());
        assertThat(yamlSuites.containsKey("suite2"), equalTo(true));
        assertEquals(2, yamlSuites.get("suite2").size());

        //multiple paths, which can be both directories or yaml test suites (with optional file extension)
        yamlSuites = ESClientYamlSuiteTestCase.loadSuites("suite2/10_basic", "suite1");
        assertThat(yamlSuites, notNullValue());
        assertThat(yamlSuites.size(), equalTo(2));
        assertThat(yamlSuites.containsKey("suite2"), equalTo(true));
        assertThat(yamlSuites.get("suite2").size(), equalTo(1));
        assertSingleFile(yamlSuites.get("suite2"), "suite2", "10_basic.yml");
        assertThat(yamlSuites.containsKey("suite1"), equalTo(true));
        assertThat(yamlSuites.get("suite1").size(), greaterThan(1));

        //files can be loaded from classpath and from file system too
        Path dir = createTempDir();
        Path file = dir.resolve("test_loading.yml");
        Files.createFile(file);
    }

    private static void assertSingleFile(Map<String, Set<Path>> yamlSuites, String dirName, String fileName) {
        assertThat(yamlSuites, notNullValue());
        assertThat(yamlSuites.size(), equalTo(1));
        assertThat(yamlSuites.containsKey(dirName), equalTo(true));
        assertSingleFile(yamlSuites.get(dirName), dirName, fileName);
    }

    private static void assertSingleFile(Set<Path> files, String dirName, String fileName) {
        assertThat(files.size(), equalTo(1));
        Path file = files.iterator().next();
        assertThat(file.getFileName().toString(), equalTo(fileName));
        assertThat(file.toAbsolutePath().getParent().getFileName().toString(), equalTo(dirName));
    }

    public void testAttachSniffedMetadataOnClientOk() {
        RestClient client = mock(RestClient.class);
        Node[] originalNodes = new Node[] {
            new Node(new HttpHost("1")),
            new Node(new HttpHost("2")),
            new Node(new HttpHost("3")),
        };
        List<Node> nodesWithMetadata = Arrays.asList(new Node[] {
            // This node matches exactly:
            new Node(new HttpHost("1"), emptyList(), randomAlphaOfLength(5), randomRoles()),
            // This node also matches exactly but has bound hosts which don't matter:
            new Node(new HttpHost("2"), Arrays.asList(new HttpHost("2"), new HttpHost("not2")),
                    randomAlphaOfLength(5), randomRoles()),
            // This node's host doesn't match but one of its published hosts does so
            // we return a modified version of it:
            new Node(new HttpHost("not3"), Arrays.asList(new HttpHost("not3"), new HttpHost("3")),
                    randomAlphaOfLength(5), randomRoles()),
            // This node isn't in the original list so it isn't added:
            new Node(new HttpHost("4"), emptyList(), randomAlphaOfLength(5), randomRoles()),
        });
        ESClientYamlSuiteTestCase.attachSniffedMetadataOnClient(client, originalNodes, nodesWithMetadata);
        verify(client).setNodes(new Node[] {
            nodesWithMetadata.get(0),
            nodesWithMetadata.get(1),
            nodesWithMetadata.get(2).withHost(new HttpHost("3")),
        });
    }

    public void testAttachSniffedMetadataOnClientNotEnoughNodes() {
        // Try a version of the call that should fail because it doesn't have all the results
        RestClient client = mock(RestClient.class);
        Node[] originalNodes = new Node[] {
            new Node(new HttpHost("1")),
            new Node(new HttpHost("2")),
        };
        List<Node> nodesWithMetadata = Arrays.asList(new Node[] {
            // This node matches exactly:
            new Node(new HttpHost("1"), emptyList(), "v", new Node.Roles(true, true, true)),
        });
        IllegalStateException e = expectThrows(IllegalStateException.class, () ->
                ESClientYamlSuiteTestCase.attachSniffedMetadataOnClient(client, originalNodes, nodesWithMetadata));
        assertEquals(e.getMessage(), "Didn't sniff metadata for all nodes. Wanted metadata for "
                + "[http://1, http://2] but got [[host=http://1, bound=[], version=v, roles=mdi]]");
        verify(client, never()).setNodes(any(Node[].class));
    }

    private Node.Roles randomRoles() {
        return new Node.Roles(randomBoolean(), randomBoolean(), randomBoolean());
    }
}
