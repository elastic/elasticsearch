/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.test.rest.yaml;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.yaml.section.ExecutableSection;
import org.elasticsearch.xcontent.XContentLocation;
import org.junit.Before;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ESClientYamlSuitErrorCollectorTests extends ESTestCase {

    private ESClientYamlSuitErrorCollector collector;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        ClientYamlTestExecutionContext mockContext = mock(ClientYamlTestExecutionContext.class);
        ClientYamlTestCandidate mockCandidate = mock(ClientYamlTestCandidate.class);
        when(mockCandidate.getSuitePath()).thenReturn("test/suite");
        collector = new ESClientYamlSuitErrorCollector(mockContext, mockCandidate);
    }

    public void testMultipleFailuresCollectedTogether() {
        ExecutableSection section1 = createFailingAssertion(10, "Failure 1");
        ExecutableSection section2 = createFailingAssertion(20, "Failure 2");
        ExecutableSection section3 = createFailingAssertion(30, "Failure 3");

        collector.checkSucceeds(section1);
        collector.checkSucceeds(section2);
        collector.checkSucceeds(section3);

        AssertionError error = expectThrows(AssertionError.class, collector::verify);
        String message = error.getMessage();

        assertThat(message, containsString("Failure 1"));
        assertThat(message, containsString("Failure 2"));
        assertThat(message, containsString("Failure 3"));
        assertThat(message, containsString("There were 3 errors"));
    }

    public void testNonAssertionErrorWrapped() throws IOException {
        ExecutableSection section = new ExecutableSection() {
            @Override
            public XContentLocation getLocation() {
                return new XContentLocation(15, 1);
            }

            @Override
            public void execute(ClientYamlTestExecutionContext executionContext) throws IOException {
                throw new IOException("IO error occurred");
            }
        };

        collector.checkSucceeds(section);

        AssertionError error = expectThrows(AssertionError.class, collector::verify);
        String message = error.getMessage();

        assertThat("Error message should contain suite path", message, containsString("test/suite"));
        assertThat("Error message should contain line number", message, containsString("15"));
        assertThat("Error message should contain original error", message, containsString("IO error occurred"));
    }

    private ExecutableSection createFailingAssertion(int lineNumber, String message) {
        return new ExecutableSection() {
            @Override
            public XContentLocation getLocation() {
                return new XContentLocation(lineNumber, 1);
            }

            @Override
            public void execute(ClientYamlTestExecutionContext executionContext) throws IOException {
                throw new AssertionError(message);
            }
        };
    }
}
