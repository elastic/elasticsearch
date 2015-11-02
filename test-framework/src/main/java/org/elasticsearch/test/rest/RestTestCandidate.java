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
package org.elasticsearch.test.rest;

import org.elasticsearch.test.rest.section.RestTestSuite;
import org.elasticsearch.test.rest.section.SetupSection;
import org.elasticsearch.test.rest.section.TestSection;

/**
 * Wraps {@link org.elasticsearch.test.rest.section.TestSection}s ready to be run.
 * Each test section is associated to its {@link org.elasticsearch.test.rest.section.RestTestSuite}.
 */
public class RestTestCandidate {

    private final RestTestSuite restTestSuite;
    private final TestSection testSection;

    public RestTestCandidate(RestTestSuite restTestSuite, TestSection testSection) {
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

    public TestSection getTestSection() {
        return testSection;
    }

    @Override
    public String toString() {
        return getTestPath();
    }
}
