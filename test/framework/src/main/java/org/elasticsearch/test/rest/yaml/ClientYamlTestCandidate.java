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

import org.elasticsearch.test.rest.yaml.section.ClientYamlTestSuite;
import org.elasticsearch.test.rest.yaml.section.SetupSection;
import org.elasticsearch.test.rest.yaml.section.TeardownSection;
import org.elasticsearch.test.rest.yaml.section.ClientYamlTestSection;

/**
 * Wraps {@link ClientYamlTestSection}s ready to be run. Each test section is associated to its {@link ClientYamlTestSuite}.
 */
public class ClientYamlTestCandidate {

    private final ClientYamlTestSuite restTestSuite;
    private final ClientYamlTestSection testSection;

    public ClientYamlTestCandidate(ClientYamlTestSuite restTestSuite, ClientYamlTestSection testSection) {
        this.restTestSuite = restTestSuite;
        this.testSection = testSection;
    }

    public String getApi() {
        return restTestSuite.getApi();
    }

    public String getName() {
        return restTestSuite.getName();
    }

    public String getSuitePath() {
        return restTestSuite.getPath();
    }

    public String getTestPath() {
        return restTestSuite.getPath() + "/" + testSection.getName();
    }

    public SetupSection getSetupSection() {
        return restTestSuite.getSetupSection();
    }

    public TeardownSection getTeardownSection() {
        return restTestSuite.getTeardownSection();
    }

    public ClientYamlTestSection getTestSection() {
        return testSection;
    }

    @Override
    public String toString() {
        return getTestPath();
    }
}
