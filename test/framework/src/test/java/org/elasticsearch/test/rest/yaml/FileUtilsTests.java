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

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.yaml.FileUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.greaterThan;

public class FileUtilsTests extends ESTestCase {
    public void testLoadSingleYamlSuite() throws Exception {
        Map<String,Set<Path>> yamlSuites = FileUtils.findYamlSuites(null, "/rest-api-spec/test", "/rest-api-spec/test/suite1/10_basic");
        assertSingleFile(yamlSuites, "suite1", "10_basic.yaml");

        //the path prefix is optional
        yamlSuites = FileUtils.findYamlSuites(null, "/rest-api-spec/test", "suite1/10_basic.yaml");
        assertSingleFile(yamlSuites, "suite1", "10_basic.yaml");

        //extension .yaml is optional
        yamlSuites = FileUtils.findYamlSuites(null, "/rest-api-spec/test", "suite1/10_basic");
        assertSingleFile(yamlSuites, "suite1", "10_basic.yaml");
    }

    public void testLoadMultipleYamlSuites() throws Exception {
        //single directory
        Map<String,Set<Path>> yamlSuites = FileUtils.findYamlSuites(null, "/rest-api-spec/test", "suite1");
        assertThat(yamlSuites, notNullValue());
        assertThat(yamlSuites.size(), equalTo(1));
        assertThat(yamlSuites.containsKey("suite1"), equalTo(true));
        assertThat(yamlSuites.get("suite1").size(), greaterThan(1));

        //multiple directories
        yamlSuites = FileUtils.findYamlSuites(null, "/rest-api-spec/test", "suite1", "suite2");
        assertThat(yamlSuites, notNullValue());
        assertThat(yamlSuites.size(), equalTo(2));
        assertThat(yamlSuites.containsKey("suite1"), equalTo(true));
        assertEquals(2, yamlSuites.get("suite1").size());
        assertThat(yamlSuites.containsKey("suite2"), equalTo(true));
        assertEquals(2, yamlSuites.get("suite2").size());

        //multiple paths, which can be both directories or yaml test suites (with optional file extension)
        yamlSuites = FileUtils.findYamlSuites(null, "/rest-api-spec/test", "suite2/10_basic", "suite1");
        assertThat(yamlSuites, notNullValue());
        assertThat(yamlSuites.size(), equalTo(2));
        assertThat(yamlSuites.containsKey("suite2"), equalTo(true));
        assertThat(yamlSuites.get("suite2").size(), equalTo(1));
        assertSingleFile(yamlSuites.get("suite2"), "suite2", "10_basic.yaml");
        assertThat(yamlSuites.containsKey("suite1"), equalTo(true));
        assertThat(yamlSuites.get("suite1").size(), greaterThan(1));

        //files can be loaded from classpath and from file system too
        Path dir = createTempDir();
        Path file = dir.resolve("test_loading.yaml");
        Files.createFile(file);

        //load from directory outside of the classpath
        yamlSuites = FileUtils.findYamlSuites(dir.getFileSystem(), "/rest-api-spec/test", dir.toAbsolutePath().toString());
        assertThat(yamlSuites, notNullValue());
        assertThat(yamlSuites.size(), equalTo(1));
        assertThat(yamlSuites.containsKey(dir.getFileName().toString()), equalTo(true));
        assertSingleFile(yamlSuites.get(dir.getFileName().toString()), dir.getFileName().toString(), file.getFileName().toString());

        //load from external file (optional extension)
        yamlSuites = FileUtils.findYamlSuites(dir.getFileSystem(), "/rest-api-spec/test",
                dir.resolve("test_loading").toAbsolutePath().toString());
        assertThat(yamlSuites, notNullValue());
        assertThat(yamlSuites.size(), equalTo(1));
        assertThat(yamlSuites.containsKey(dir.getFileName().toString()), equalTo(true));
        assertSingleFile(yamlSuites.get(dir.getFileName().toString()), dir.getFileName().toString(), file.getFileName().toString());
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
}
