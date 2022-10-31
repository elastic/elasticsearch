/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.smoketest;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;

public class SemanticSearchReadPermissionsIT extends AbstractSemanticSearchPermissionsIT {

    public SemanticSearchReadPermissionsIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Override
    protected String[] getCredentials() {
        return new String[] { "read_index_no_ml", "read_index_no_ml_password" };
    }
}
