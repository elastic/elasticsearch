/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.gen.argument;

import com.squareup.javapoet.TypeName;

import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.test.ESTestCase;

public class JitConstantFixedArgumentTests extends ESTestCase {

    public void testRejectsThreadLocalScope() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new JitConstantFixedArgument(TypeName.LONG, "rhs", true, Fixed.Scope.THREAD_LOCAL, false)
        );
        assertTrue(
            "message should name the parameter and the offending scope: " + e.getMessage(),
            e.getMessage().contains("rhs") && e.getMessage().contains("THREAD_LOCAL")
        );
    }

    public void testAcceptsSingletonScope() {
        new JitConstantFixedArgument(TypeName.LONG, "rhs", true, Fixed.Scope.SINGLETON, false);
    }
}
