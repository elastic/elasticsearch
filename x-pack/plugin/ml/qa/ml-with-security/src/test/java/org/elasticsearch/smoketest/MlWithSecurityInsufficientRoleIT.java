/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.smoketest;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;

public class MlWithSecurityInsufficientRoleIT extends MlWithSecurityIT {

    public MlWithSecurityInsufficientRoleIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Override
    public void test() throws IOException {
        try {
            // Cannot use expectThrows here because blacklisted tests will throw an
            // InternalAssumptionViolatedException rather than an AssertionError
            super.test();
            fail("should have failed because of missing role");
        } catch (AssertionError ae) {
            // Some tests assert on searches of wildcarded ML indices rather than on ML endpoints.  For these we expect no hits.
            if (ae.getMessage().contains("hits.total didn't match expected value")) {
                assertThat(ae.getMessage(), containsString("but was Integer [0]"));
            } else {
                assertThat(ae.getMessage(),
                        either(containsString("action [cluster:monitor/xpack/ml")).or(containsString("action [cluster:admin/xpack/ml")));
                assertThat(ae.getMessage(), containsString("returned [403 Forbidden]"));
                assertThat(ae.getMessage(), containsString("is unauthorized for user [no_ml]"));
            }
        }
    }

    @Override
    protected String[] getCredentials() {
        return new String[]{"no_ml", "x-pack-test-password"};
    }
}

