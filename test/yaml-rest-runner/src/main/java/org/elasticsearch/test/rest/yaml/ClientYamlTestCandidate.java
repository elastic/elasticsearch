/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.rest.yaml;

import org.elasticsearch.test.rest.yaml.section.ClientYamlTestSection;
import org.elasticsearch.test.rest.yaml.section.ClientYamlTestSuite;
import org.elasticsearch.test.rest.yaml.section.SetupSection;
import org.elasticsearch.test.rest.yaml.section.TeardownSection;

/**
 * Wraps {@link ClientYamlTestSection}s ready to be run. Each test section is associated to its {@link ClientYamlTestSuite}.
 */
public class ClientYamlTestCandidate {

    private final ClientYamlTestSuite restTestSuite;
    private final ClientYamlTestSection testSection;

    public ClientYamlTestCandidate(ClientYamlTestSuite restTestSuite, ClientYamlTestSection testSection) {
        this.restTestSuite = restTestSuite;
        this.testSection = testSection;
    }

    public String getApi() {
        return restTestSuite.getApi();
    }

    public String getName() {
        return restTestSuite.getName();
    }

    public String getSuitePath() {
        return restTestSuite.getPath();
    }

    public String getTestPath() {
        return restTestSuite.getPath() + "/" + testSection.getName();
    }

    public SetupSection getSetupSection() {
        return restTestSuite.getSetupSection();
    }

    public ClientYamlTestSuite getRestTestSuite() {
        return restTestSuite;
    }

    public TeardownSection getTeardownSection() {
        return restTestSuite.getTeardownSection();
    }

    public ClientYamlTestSection getTestSection() {
        return testSection;
    }

    @Override
    public String toString() {
        return getTestPath();
    }
}
