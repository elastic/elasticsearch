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

package org.elasticsearch.packaging.test;

import org.apache.http.client.fluent.Request;
import org.elasticsearch.packaging.util.Distribution;
import org.elasticsearch.packaging.util.FileUtils;
import org.elasticsearch.packaging.util.Platforms;
import org.junit.Before;

import static org.elasticsearch.packaging.util.FileUtils.append;
import static org.elasticsearch.packaging.util.ServerUtils.makeRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assume.assumeTrue;

public class ConfigurationTests extends PackagingTestCase {

    @Before
    public void filterDistros() {
        assumeTrue("no docker", distribution.packaging != Distribution.Packaging.DOCKER);
    }

    public void test10Install() throws Exception {
        install();
    }

    public void test60HostnameSubstitution() throws Exception {
        String hostnameKey = Platforms.WINDOWS ? "COMPUTERNAME" : "HOSTNAME";
        sh.getEnv().put(hostnameKey, "mytesthost");
        withCustomConfig(confPath -> {
            FileUtils.append(confPath.resolve("elasticsearch.yml"), "node.name: ${HOSTNAME}");
            if (distribution.isPackage()) {
                append(installation.envFile, "HOSTNAME=mytesthost");
            }
            assertWhileRunning(() -> {
                final String nameResponse = makeRequest(Request.Get("http://localhost:9200/_cat/nodes?h=name")).strip();
                assertThat(nameResponse, equalTo("mytesthost"));
            });
        });
    }
}
