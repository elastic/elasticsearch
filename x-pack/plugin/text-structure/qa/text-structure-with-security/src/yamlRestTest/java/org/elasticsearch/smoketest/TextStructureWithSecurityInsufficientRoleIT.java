/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.smoketest;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.section.DoSection;
import org.elasticsearch.test.rest.yaml.section.ExecutableSection;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class TextStructureWithSecurityInsufficientRoleIT extends TextStructureWithSecurityIT {

    private final ClientYamlTestCandidate testCandidate;

    public TextStructureWithSecurityInsufficientRoleIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
        this.testCandidate = testCandidate;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void test() throws IOException {
        try {
            // Cannot use expectThrows here because blacklisted tests will throw an
            // InternalAssumptionViolatedException rather than an AssertionError
            super.test();

            // We should have got here if and only if no text structure endpoints were called
            for (ExecutableSection section : testCandidate.getTestSection().getExecutableSections()) {
                if (section instanceof DoSection) {
                    String apiName = ((DoSection) section).getApiCallSection().getApi();

                    if (apiName.startsWith("text_structure.")) {
                        fail("call to text_structure endpoint [" + apiName + "] should have failed because of missing role");
                    }
                }
            }
        } catch (AssertionError ae) {
            assertThat(ae.getMessage(), containsString("action [cluster:monitor/text_structure"));
            assertThat(ae.getMessage(), containsString("returned [403 Forbidden]"));
            assertThat(ae.getMessage(), containsString("is unauthorized for user [no_text_structure]"));
        }
    }

    @Override
    protected String[] getCredentials() {
        return new String[] { "no_text_structure", "x-pack-test-password" };
    }
}
