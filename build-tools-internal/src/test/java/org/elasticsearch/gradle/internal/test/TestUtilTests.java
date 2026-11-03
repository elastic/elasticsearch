/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test;

import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class TestUtilTests {

    @Test
    public void testDescendsIntoThePlatformDirectoryOfEveryRoot() {
        String alpha = TestUtil.getTestLibraryPath("/alpha");
        String beta = TestUtil.getTestLibraryPath("/beta");

        String both = TestUtil.getTestLibraryPath("/alpha" + File.pathSeparator + "/beta");

        assertEquals(alpha + File.pathSeparator + beta, both);
    }
}
