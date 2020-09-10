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
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;

public class MlWithSecurityInsufficientRoleIT extends MlWithSecurityIT {

    private final ClientYamlTestCandidate testCandidate;

    public MlWithSecurityInsufficientRoleIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
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

            // We should have got here if and only if no ML endpoints were called
            for (ExecutableSection section : testCandidate.getTestSection().getExecutableSections()) {
                if (section instanceof DoSection) {
                    String apiName = ((DoSection) section).getApiCallSection().getApi();

                    if (apiName.startsWith("ml.")) {
                        fail("call to ml endpoint [" + apiName + "] should have failed because of missing role");
                    } else if (apiName.startsWith("search")) {
                        DoSection doSection = (DoSection) section;
                        List<Map<String, Object>> bodies = doSection.getApiCallSection().getBodies();
                        boolean containsInferenceAgg = false;
                        for (Map<String, Object> body : bodies) {
                            Map<String, Object> aggs = (Map<String, Object>)body.get("aggs");
                            containsInferenceAgg = containsInferenceAgg || containsKey("inference", aggs);
                        }

                        if (containsInferenceAgg) {
                            fail("call to [search] with the ml inference agg should have failed because of missing role");
                        }
                    }
                }
            }

        } catch (AssertionError ae) {
            // Some tests assert on searches of wildcarded ML indices rather than on ML endpoints.  For these we expect no hits.
            if (ae.getMessage().contains("hits.total didn't match expected value")) {
                assertThat(ae.getMessage(), containsString("but was Integer [0]"));
            } else {
                assertThat(ae.getMessage(),
                    either(containsString("action [cluster:monitor/xpack/ml"))
                        .or(containsString("action [cluster:admin/xpack/ml"))
                        .or(containsString("security_exception")));
                assertThat(ae.getMessage(), containsString("returned [403 Forbidden]"));
                assertThat(ae.getMessage(),
                    either(containsString("is unauthorized for user [no_ml]"))
                        .or(containsString("user [no_ml] does not have the privilege to get trained models")));
            }
        }
    }

    @Override
    protected String[] getCredentials() {
        return new String[]{"no_ml", "x-pack-test-password"};
    }

    @SuppressWarnings("unchecked")
    static boolean containsKey(String key, Map<String, Object> mapOfMaps) {
        if (mapOfMaps.containsKey(key)) {
            return true;
        }

        Set<Map.Entry<String, Object>> entries = mapOfMaps.entrySet();
        for (Map.Entry<String, Object> entry : entries) {
            if (entry.getValue() instanceof Map<?,?>) {
                boolean isInNestedMap = containsKey(key, (Map<String, Object>)entry.getValue());
                if (isInNestedMap) {
                    return true;
                }
            }
        }

        return false;
    }
}

