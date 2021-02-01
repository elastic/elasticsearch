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
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;

public class MlWithSecurityUserRoleIT extends MlWithSecurityIT {

    /**
     * These are actions that require the monitor role and/or access to the relevant source index.
     * ml_user should have both of these in the tests.
     */
    private static final List<Pattern> ALLOWED_ACTION_PATTERNS = Arrays.asList(
        Pattern.compile("ml\\.get_.*"),
        Pattern.compile("ml\\.find_file_structure"),
        Pattern.compile("ml\\.evaluate_data_frame")
    );

    private final ClientYamlTestCandidate testCandidate;

    public MlWithSecurityUserRoleIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
        this.testCandidate = testCandidate;
    }

    @Override
    public void test() throws IOException {
        try {
            super.test();

            // We should have got here if and only if the only ML endpoints in the test were in the allowed list
            for (ExecutableSection section : testCandidate.getTestSection().getExecutableSections()) {
                if (section instanceof DoSection) {
                    String apiName = ((DoSection) section).getApiCallSection().getApi();

                    if (apiName.startsWith("ml.") && isAllowed(apiName) == false) {
                        fail("call to ml endpoint [" + apiName + "] should have failed because of missing role");
                    }
                }
            }
        } catch (AssertionError ae) {
            assertThat(ae.getMessage(),
                either(containsString("action [cluster:monitor/xpack/ml"))
                    .or(containsString("action [cluster:admin/xpack/ml"))
                    .or(containsString("action [cluster:admin/ingest")));
            assertThat(ae.getMessage(), containsString("returned [403 Forbidden]"));
            assertThat(ae.getMessage(), containsString("is unauthorized for user [ml_user]"));
        }
    }

    private static boolean isAllowed(String apiName) {
        for (Pattern pattern : ALLOWED_ACTION_PATTERNS) {
            if (pattern.matcher(apiName).find()) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected String[] getCredentials() {
        return new String[]{"ml_user", "x-pack-test-password"};
    }
}
