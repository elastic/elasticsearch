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
package org.elasticsearch.docker.test;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;

public class DockerYmlTestSuiteIT extends ESClientYamlSuiteTestCase {

    public DockerYmlTestSuiteIT(ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters();
    }

    @Override
    protected String getTestRestCluster() {
        String distribution = System.getProperty("tests.distribution", "default");
        if (distribution.equals("oss") == false && distribution.equals("default") == false) {
            throw new IllegalArgumentException("supported values for tests.distribution are oss or default but it was " + distribution);
        }
        return new StringBuilder()
            .append("localhost:")
            .append(getProperty("test.fixtures.elasticsearch-" + distribution + "-1.tcp.9200"))
            .append(",")
            .append("localhost:")
            .append(getProperty("test.fixtures.elasticsearch-" + distribution + "-2.tcp.9200"))
            .toString();
    }

    private String getProperty(String key) {
        String port = System.getProperty(key);
        if (port == null) {
            throw new IllegalStateException("Could not find system properties from test.fixtures. " +
                "This test expects to run with the elasticsearch.test.fixtures Gradle plugin");
        }
        return port;
    }
}
