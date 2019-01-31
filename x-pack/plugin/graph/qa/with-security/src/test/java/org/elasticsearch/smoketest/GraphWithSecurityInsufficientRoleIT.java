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

public class GraphWithSecurityInsufficientRoleIT extends GraphWithSecurityIT {

    public GraphWithSecurityInsufficientRoleIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    public void test() throws IOException {
        try {
            super.test();
            fail("should have failed because of missing role");
        } catch(AssertionError ae) {
            assertThat(ae.getMessage(), containsString("action [indices:data/read/xpack/graph/explore"));
            assertThat(ae.getMessage(), containsString("returned [403 Forbidden]"));
            assertThat(ae.getMessage(), containsString("is unauthorized for user [no_graph_explorer]"));
        }
    }

    @Override
    protected String[] getCredentials() {
        return new String[]{"no_graph_explorer", "x-pack-test-password"};
    }
}

