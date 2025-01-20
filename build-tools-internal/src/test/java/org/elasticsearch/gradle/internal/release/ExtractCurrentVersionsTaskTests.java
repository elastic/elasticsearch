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
import com.github.javaparser.ast.body.FieldDeclaration;

import org.elasticsearch.gradle.internal.release.ExtractCurrentVersionsTask.FieldIdExtractor;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class ExtractCurrentVersionsTaskTests {

    @Test
    public void testFieldExtractor() {
        var unit = StaticJavaParser.parse("""
            public class Version {
                public static final Version V_1 = def(1);
                public static final Version V_2 = def(2);
                public static final Version V_3 = def(3);

                // ignore fields with no or more than one int
                public static final Version REF = V_3;
                public static final Version COMPUTED = compute(100, 200);
            }""");

        FieldIdExtractor extractor = new FieldIdExtractor();
        unit.walk(FieldDeclaration.class, extractor);
        assertThat(extractor.highestVersionId(), is(3));
    }
}
