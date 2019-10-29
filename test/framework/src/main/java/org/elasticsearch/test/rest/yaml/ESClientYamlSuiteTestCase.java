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

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;
import org.apache.lucene.util.TimeUnits;

import java.io.IOException;

/**
 * Runs a suite of yaml tests shared with all the official Elasticsearch
 * clients against against an elasticsearch cluster.
 */
@TimeoutSuite(millis = 30 * TimeUnits.MINUTE)
public abstract class ESClientYamlSuiteTestCase extends BaseESClientYamlSuiteTestCase {

    private ClientYamlTestCandidate testCandidate;

    protected ESClientYamlSuiteTestCase(ClientYamlTestCandidate testCandidate) {
        this.testCandidate = testCandidate;
    }

    public void test() throws IOException {
        runTest(testCandidate);
    }
}
