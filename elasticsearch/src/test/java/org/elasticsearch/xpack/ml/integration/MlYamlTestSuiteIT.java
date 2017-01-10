/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.After;

import java.io.IOException;

/** Rest integration test. Runs against a cluster started by {@code gradle integTest} */
public class MlYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    public MlYamlTestSuiteIT(ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws IOException {
        return createParameters();
    }

    @After
    public void clearMlState() throws IOException {
        new MlRestTestStateCleaner(client(), this).clearMlMetadata();
    }
}
