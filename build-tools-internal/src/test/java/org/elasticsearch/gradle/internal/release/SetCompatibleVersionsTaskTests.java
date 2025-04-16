/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.release;

import com.github.javaparser.StaticJavaParser;
import com.github.javaparser.ast.CompilationUnit;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasToString;

public class SetCompatibleVersionsTaskTests {

    @Test
    public void updateMinCcsVersion() {
        final String transportVersionsJava = """
            public class TransportVersions {
                public static final TransportVersion V1 = def(100);
                public static final TransportVersion V2 = def(200);
                public static final TransportVersion V3 = def(300);

                public static final TransportVersion MINIMUM_CCS_VERSION = V2;
            }""";
        final String updatedJava = """
            public class TransportVersions {

                public static final TransportVersion V1 = def(100);

                public static final TransportVersion V2 = def(200);

                public static final TransportVersion V3 = def(300);

                public static final TransportVersion MINIMUM_CCS_VERSION = V3;
            }
            """;

        CompilationUnit unit = StaticJavaParser.parse(transportVersionsJava);

        SetCompatibleVersionsTask.setMinimumCcsTransportVersion(unit, 300);

        assertThat(unit, hasToString(updatedJava));
    }
}
