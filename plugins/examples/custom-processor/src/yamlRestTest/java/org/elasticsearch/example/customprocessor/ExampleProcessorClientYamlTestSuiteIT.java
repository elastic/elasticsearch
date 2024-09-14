/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.example.customprocessor;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;

/**
 * {@link ExampleProcessorClientYamlTestSuiteIT} executes the plugin's REST API integration tests.
 * <p>
 * The tests can be executed using the command: ./gradlew :custom-processor:yamlRestTest
 * <p>
 * This class extends {@link ESClientYamlSuiteTestCase}, which takes care of parsing the YAML files
 * located in the src/yamlRestTest/resources/rest-api-spec/test/ directory and validates them against the
 * custom REST API definition files located in src/yamlRestTest/resources/rest-api-spec/api/.
 * <p>
 * Once validated, {@link ESClientYamlSuiteTestCase} executes the REST tests against a single node
 * integration cluster which has the plugin already installed by the Gradle build script.
 * </p>
 */
public class ExampleProcessorClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    public ExampleProcessorClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        // The test executes all the test candidates by default
        // see ESClientYamlSuiteTestCase.REST_TESTS_SUITE
        return ESClientYamlSuiteTestCase.createParameters();
    }
}
