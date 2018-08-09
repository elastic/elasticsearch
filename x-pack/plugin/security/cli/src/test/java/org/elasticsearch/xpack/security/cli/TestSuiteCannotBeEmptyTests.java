/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.cli;

import org.elasticsearch.test.ESTestCase;

public class TestSuiteCannotBeEmptyTests extends ESTestCase {

    public void testIsHereSoThisSuiteIsNonEmpty() {
        // This is needed on a FIPS JVM as the rest of the suite is skipped
    }
}
