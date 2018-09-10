/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.smoketest;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.section.DoSection;
import org.elasticsearch.test.rest.yaml.section.ExecutableSection;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;

public class MlWithSecurityUserRoleIT extends MlWithSecurityIT {

    private final ClientYamlTestCandidate testCandidate;

    public MlWithSecurityUserRoleIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
        this.testCandidate = testCandidate;
    }

    @Override
    public void test() throws IOException {
        try {
            super.test();

            // We should have got here if and only if the only ML endpoints in the test were GETs
            // or the find_file_structure API, which is also available to the machine_learning_user
            // role
            for (ExecutableSection section : testCandidate.getTestSection().getExecutableSections()) {
                if (section instanceof DoSection) {
                    if (((DoSection) section).getApiCallSection().getApi().startsWith("xpack.ml.") &&
                            ((DoSection) section).getApiCallSection().getApi().startsWith("xpack.ml.get_") == false &&
                            ((DoSection) section).getApiCallSection().getApi().equals("xpack.ml.find_file_structure") == false) {
                        fail("should have failed because of missing role");
                    }
                }
            }
        } catch (AssertionError ae) {
            assertThat(ae.getMessage(),
                    either(containsString("action [cluster:monitor/xpack/ml")).or(containsString("action [cluster:admin/xpack/ml")));
            assertThat(ae.getMessage(), containsString("returned [403 Forbidden]"));
            assertThat(ae.getMessage(), containsString("is unauthorized for user [ml_user]"));
        }
    }

    @Override
    protected String[] getCredentials() {
        return new String[]{"ml_user", "x-pack-test-password"};
    }
}

