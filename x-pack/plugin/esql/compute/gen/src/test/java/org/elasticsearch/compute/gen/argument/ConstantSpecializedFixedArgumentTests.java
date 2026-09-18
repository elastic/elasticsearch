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

/**
 * Tests for {@link ConstantSpecializedFixedArgument#validateJitConstantCompatible} — the codegen-time
 * guard that enforces the two preconditions for {@code @Fixed(jitConstant=true)}: SINGLETON scope only,
 * and non-Releasable parameter type only. Both invariants are required for the specialized-subclass
 * lifecycle model (see Javadoc on the validator) and a violation produces incorrect runtime behavior
 * — so we hard-fail at codegen time rather than at query time.
 */
public class ConstantSpecializedFixedArgumentTests extends ESTestCase {

    // ----- Scope guard -----

    public void testRejectsThreadLocalScope() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ConstantSpecializedFixedArgument.validateJitConstantCompatible("rhs", Fixed.Scope.THREAD_LOCAL, /* releasable */ false)
        );
        assertTrue(
            "message should name the parameter and the offending scope: " + e.getMessage(),
            e.getMessage().contains("rhs") && e.getMessage().contains("THREAD_LOCAL")
        );
    }

    public void testAcceptsSingletonScope() {
        ConstantSpecializedFixedArgument.validateJitConstantCompatible("rhs", Fixed.Scope.SINGLETON, /* releasable */ false);
    }

    // ----- Releasable guard -----

    public void testRejectsReleasableType() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ConstantSpecializedFixedArgument.validateJitConstantCompatible("pat", Fixed.Scope.SINGLETON, /* releasable */ true)
        );
        assertTrue(
            "message should name the parameter and explain the lifecycle reason: " + e.getMessage(),
            e.getMessage().contains("pat") && e.getMessage().contains("Releasable")
        );
    }

    public void testRejectsReleasableTypeWithBadScope() {
        // Scope check fires first; the order doesn't matter functionally — both are violations and
        // either thrown error is sufficient. This test just pins that one of the two messages comes
        // out rather than something undefined.
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ConstantSpecializedFixedArgument.validateJitConstantCompatible("pat", Fixed.Scope.THREAD_LOCAL, /* releasable */ true)
        );
        assertTrue(
            "message should name at least one violated invariant: " + e.getMessage(),
            e.getMessage().contains("SINGLETON") || e.getMessage().contains("Releasable")
        );
    }

    // ----- Record construction -----

    public void testRecordConstructsWithoutValidator() {
        // The record itself has no validation — it's a pure data carrier. Validation is the
        // caller's responsibility (Argument.fromFixed calls validateJitConstantCompatible
        // before constructing). This test pins that the record stays pure-data and doesn't
        // re-validate at construction time.
        ConstantSpecializedFixedArgument arg = new ConstantSpecializedFixedArgument(TypeName.LONG, "rhs", true, Fixed.Scope.SINGLETON);
        assertEquals("rhs", arg.name());
        assertEquals(Fixed.Scope.SINGLETON, arg.scope());
    }
}
