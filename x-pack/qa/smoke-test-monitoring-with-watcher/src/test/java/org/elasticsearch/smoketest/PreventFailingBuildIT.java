/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.smoketest;

import org.elasticsearch.test.ESIntegTestCase;

public class PreventFailingBuildIT extends ESIntegTestCase {

    public void testSoThatTestsDoNotFail() {
        // Noop

        // This is required because if tests are not enable no
        // tests will be run in the entire project and all tests will fail.
    }
}
