/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.release;

import com.github.javaparser.StaticJavaParser;
import com.github.javaparser.ast.CompilationUnit;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasToString;

public class SetMinCCSVersionTaskTests {

    @Test
    public void setCcsVersion() {
        final String transportVersionsJava = """
            public class TransportVersions {
                public static final TransportVersion V_1 = 10;
                public static final TransportVersion V_2 = 20;
                public static final TransportVersion V_3 = 30;
                public static final TransportVersion V_4 = 40;

                public static final TransportVersion MINIMUM_CCS_VERSION = V_3;
            }""";
        final String updatedTransportVersionsJava = """
            public class TransportVersions {

                public static final TransportVersion V_1 = 10;

                public static final TransportVersion V_2 = 20;

                public static final TransportVersion V_3 = 30;

                public static final TransportVersion V_4 = 40;

                public static final TransportVersion MINIMUM_CCS_VERSION = V_4;
            }
            """;

        CompilationUnit unit = StaticJavaParser.parse(transportVersionsJava);

        SetMinCCSVersionTask.setMinCcsVersionField(unit, 40);

        assertThat(unit, hasToString(updatedTransportVersionsJava));
    }
}
