/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.integration;

import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.test.rest.yaml.parser.ClientYamlTestParseException;
import org.junit.After;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import java.io.IOException;

import static org.elasticsearch.xpack.prelert.integration.ScheduledJobIT.clearPrelertMetadata;

/** Rest integration test. Runs against a cluster started by {@code gradle integTest} */
public class PrelertYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    public PrelertYamlTestSuiteIT(ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws IOException, ClientYamlTestParseException {
        return createParameters();
    }

    @After
    public void clearPrelertState() throws IOException {
        clearPrelertMetadata(adminClient());
    }

}
