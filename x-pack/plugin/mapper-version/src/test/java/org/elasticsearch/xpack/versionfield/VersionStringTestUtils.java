/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.versionfield;

import org.elasticsearch.test.ESTestCase;

public class VersionStringTestUtils {
    public static String randomVersionString() {
        return randomVersionNumber() + (ESTestCase.randomBoolean() ? "" : randomPrerelease());
    }

    private static String randomVersionNumber() {
        int numbers = ESTestCase.between(1, 3);
        String v = Integer.toString(ESTestCase.between(0, 100));
        for (int i = 1; i < numbers; i++) {
            v += "." + ESTestCase.between(0, 100);
        }
        return v;
    }

    private static String randomPrerelease() {
        if (ESTestCase.rarely()) {
            return ESTestCase.randomFrom("alpha", "beta", "prerelease", "whatever");
        }
        return ESTestCase.randomFrom("alpha", "beta", "") + randomVersionNumber();
    }

}
