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
package org.elasticsearch.test.rest.test;

import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.rest.support.FileUtils;
import org.junit.Test;

import java.io.File;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.greaterThan;

public class FileUtilsTests extends ElasticsearchTestCase {

    @Test
    public void testLoadSingleYamlSuite() throws Exception {
        Map<String,Set<File>> yamlSuites = FileUtils.findYamlSuites("/rest-api-spec/test", "/rest-api-spec/test/get/10_basic");
        assertSingleFile(yamlSuites, "get", "10_basic.yaml");

        //the path prefix is optional
        yamlSuites = FileUtils.findYamlSuites("/rest-api-spec/test", "get/10_basic.yaml");
        assertSingleFile(yamlSuites, "get", "10_basic.yaml");

        //extension .yaml is optional
        yamlSuites = FileUtils.findYamlSuites("/rest-api-spec/test", "get/10_basic");
        assertSingleFile(yamlSuites, "get", "10_basic.yaml");
    }

    @Test
    public void testLoadMultipleYamlSuites() throws Exception {
        //single directory
        Map<String,Set<File>> yamlSuites = FileUtils.findYamlSuites("/rest-api-spec/test", "get");
        assertThat(yamlSuites, notNullValue());
        assertThat(yamlSuites.size(), equalTo(1));
        assertThat(yamlSuites.containsKey("get"), equalTo(true));
        assertThat(yamlSuites.get("get").size(), greaterThan(1));

        //multiple directories
        yamlSuites = FileUtils.findYamlSuites("/rest-api-spec/test", "get", "index");
        assertThat(yamlSuites, notNullValue());
        assertThat(yamlSuites.size(), equalTo(2));
        assertThat(yamlSuites.containsKey("get"), equalTo(true));
        assertThat(yamlSuites.get("get").size(), greaterThan(1));
        assertThat(yamlSuites.containsKey("index"), equalTo(true));
        assertThat(yamlSuites.get("index").size(), greaterThan(1));

        //multiple paths, which can be both directories or yaml test suites (with optional file extension)
        yamlSuites = FileUtils.findYamlSuites("/rest-api-spec/test", "indices.optimize/10_basic", "index");
        assertThat(yamlSuites, notNullValue());
        assertThat(yamlSuites.size(), equalTo(2));
        assertThat(yamlSuites.containsKey("indices.optimize"), equalTo(true));
        assertThat(yamlSuites.get("indices.optimize").size(), equalTo(1));
        assertSingleFile(yamlSuites.get("indices.optimize"), "indices.optimize", "10_basic.yaml");
        assertThat(yamlSuites.containsKey("index"), equalTo(true));
        assertThat(yamlSuites.get("index").size(), greaterThan(1));

        //files can be loaded from classpath and from file system too
        File dir = newTempDir();
        File file = new File(dir, "test_loading.yaml");
        assertThat(file.createNewFile(), equalTo(true));

        //load from directory outside of the classpath
        yamlSuites = FileUtils.findYamlSuites("/rest-api-spec/test", "get/10_basic", dir.getAbsolutePath());
        assertThat(yamlSuites, notNullValue());
        assertThat(yamlSuites.size(), equalTo(2));
        assertThat(yamlSuites.containsKey("get"), equalTo(true));
        assertThat(yamlSuites.get("get").size(), equalTo(1));
        assertSingleFile(yamlSuites.get("get"), "get", "10_basic.yaml");
        assertThat(yamlSuites.containsKey(dir.getName()), equalTo(true));
        assertSingleFile(yamlSuites.get(dir.getName()), dir.getName(), file.getName());

        //load from external file (optional extension)
        yamlSuites = FileUtils.findYamlSuites("/rest-api-spec/test", "get/10_basic", dir.getAbsolutePath() + File.separator + "test_loading");
        assertThat(yamlSuites, notNullValue());
        assertThat(yamlSuites.size(), equalTo(2));
        assertThat(yamlSuites.containsKey("get"), equalTo(true));
        assertThat(yamlSuites.get("get").size(), equalTo(1));
        assertSingleFile(yamlSuites.get("get"), "get", "10_basic.yaml");
        assertThat(yamlSuites.containsKey(dir.getName()), equalTo(true));
        assertSingleFile(yamlSuites.get(dir.getName()), dir.getName(), file.getName());
    }

    private static void assertSingleFile(Map<String, Set<File>> yamlSuites, String dirName, String fileName) {
        assertThat(yamlSuites, notNullValue());
        assertThat(yamlSuites.size(), equalTo(1));
        assertThat(yamlSuites.containsKey(dirName), equalTo(true));
        assertSingleFile(yamlSuites.get(dirName), dirName, fileName);
    }

    private static void assertSingleFile(Set<File> files, String dirName, String fileName) {
        assertThat(files.size(), equalTo(1));
        File file = files.iterator().next();
        assertThat(file.getName(), equalTo(fileName));
        assertThat(file.getParentFile().getName(), equalTo(dirName));
    }
}
