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

public class SemanticSearchNoReadPermissionsIT extends AbstractSemanticSearchPermissionsIT {

    private static final String USERNAME = "no_read_index_no_ml";

    private final ClientYamlTestCandidate testCandidate;

    public SemanticSearchNoReadPermissionsIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
        this.testCandidate = testCandidate;
    }

    @Override
    protected String[] getCredentials() {
        return new String[] { USERNAME, "no_read_index_no_ml_password" };
    }

    @Override
    public void test() throws IOException {
        try {
            // Cannot use expectThrows here because blacklisted tests will throw an
            // InternalAssumptionViolatedException rather than an AssertionError
            super.test();

            for (ExecutableSection section : testCandidate.getTestSection().getExecutableSections()) {
                if (section instanceof DoSection doSection) {
                    String apiName = doSection.getApiCallSection().getApi();
                    fail("call to semantic_search endpoint [" + apiName + "] should have failed because of missing role");
                }
            }
        } catch (AssertionError ae) {
            if (ae.getMessage().startsWith("call to")) {
                // rethrow the fail() from the try section above
                throw ae;
            }
            assertThat(ae.getMessage(), containsString("security_exception"));
            assertThat(ae.getMessage(), containsString("returned [403 Forbidden]"));
            assertThat(ae.getMessage(), containsString("is unauthorized for user [" + USERNAME + "]"));
        }
    }
}
