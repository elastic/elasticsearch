/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.test.ESTestCase;

import java.util.Optional;

import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresent;
import static org.hamcrest.Matchers.not;

public class VectorSimilarityFunctionsTests extends ESTestCase {

    final Optional<VectorSimilarityFunctions> vectorSimilarityFunctions;

    public VectorSimilarityFunctionsTests() {
        logger.info(platformMsg());
        vectorSimilarityFunctions = NativeAccess.instance().getVectorSimilarityFunctions();
    }

    public void testSupported() {
        supported();
    }

    protected VectorSimilarityFunctions getVectorDistance() {
        return vectorSimilarityFunctions.get();
    }

    public boolean supported() {
        var jdkVersion = Runtime.version().feature();
        var arch = System.getProperty("os.arch");
        var osName = System.getProperty("os.name");

        if (jdkVersion >= 21 && arch.equals("aarch64") && (osName.startsWith("Mac") || osName.equals("Linux"))) {
            assertThat(vectorSimilarityFunctions, isPresent());
            return true;
        } else {
            assertThat(vectorSimilarityFunctions, not(isPresent()));
            return false;
        }
    }

    public static String notSupportedMsg() {
        return "Not supported on [" + platformMsg() + "]";
    }

    public static String platformMsg() {
        var jdkVersion = Runtime.version().feature();
        var arch = System.getProperty("os.arch");
        var osName = System.getProperty("os.name");
        return "JDK=" + jdkVersion + ", os=" + osName + ", arch=" + arch;
    }
}
