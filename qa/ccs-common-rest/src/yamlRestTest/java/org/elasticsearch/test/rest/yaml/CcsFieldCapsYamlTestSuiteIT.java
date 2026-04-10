/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.rest.yaml;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;

import java.io.IOException;

/**
 * Runs the field_caps API tests only. See {@link TestSuiteApiCheck}
 */
@TimeoutSuite(millis = 20 * TimeUnits.MINUTE)
public class CcsFieldCapsYamlTestSuiteIT extends CcsCommonYamlTestSuiteIT {
    public CcsFieldCapsYamlTestSuiteIT(ClientYamlTestCandidate testCandidate) throws IOException {
        super(testCandidate);
    }
}
