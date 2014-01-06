/*
 * Licensed to Elasticsearch under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elasticsearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.test.rest.junit;

import org.elasticsearch.test.rest.section.RestTestSuite;
import org.elasticsearch.test.rest.section.SetupSection;
import org.elasticsearch.test.rest.section.TestSection;
import org.junit.runner.Description;

/**
 * Wraps {@link org.elasticsearch.test.rest.section.TestSection}s ready to be run,
 * properly enriched with the needed execution information.
 * The tests tree structure gets flattened to the leaves (test sections)
 */
public class RestTestCandidate {

    private final RestTestSuite restTestSuite;
    private final Description suiteDescription;
    private final TestSection testSection;
    private final Description testDescription;
    private final long seed;

    static RestTestCandidate empty(RestTestSuite restTestSuite, Description suiteDescription) {
        return new RestTestCandidate(restTestSuite, suiteDescription, null, null, -1);
    }

    RestTestCandidate(RestTestSuite restTestSuite, Description suiteDescription,
                      TestSection testSection, Description testDescription, long seed) {
        this.restTestSuite = restTestSuite;
        this.suiteDescription = suiteDescription;
        this.testSection = testSection;
        this.testDescription = testDescription;
        this.seed = seed;
    }

    public String getApi() {
        return restTestSuite.getApi();
    }

    public String getName() {
        return restTestSuite.getName();
    }

    public String getSuiteDescription() {
        return restTestSuite.getDescription();
    }

    public Description describeSuite() {
        return suiteDescription;
    }

    public Description describeTest() {
        return testDescription;
    }

    public SetupSection getSetupSection() {
        return restTestSuite.getSetupSection();
    }

    public TestSection getTestSection() {
        return testSection;
    }

    public long getSeed() {
        return seed;
    }
}
