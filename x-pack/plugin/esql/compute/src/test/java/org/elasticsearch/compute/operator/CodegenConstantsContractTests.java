/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.ann.CodegenConstants;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.test.ESTestCase;

/**
 * STOP. These tests guard the FQN string constants in {@link CodegenConstants} against
 * silent drift. The annotation processor in {@code compute/gen} cannot reference
 * {@code Releasable.class} (it works at compile time on {@code TypeMirror}, not
 * {@code Class}), so it uses an FQN string. If anyone renames or moves
 * {@link Releasable}, this test fails loudly in CI before the next merge — versus the
 * annotation-processor side which would silently stop matching the type and produce
 * subtly-wrong codegen.
 *
 * <p>If a test below fails because a type was renamed: update {@link CodegenConstants}
 * to the new FQN in the same commit as the rename. Do not silence the test.
 */
public class CodegenConstantsContractTests extends ESTestCase {

    public void testReleasableFqnMatchesActualClass() {
        assertEquals(
            "CodegenConstants.RELEASABLE_FQN drifted from the real Releasable class — "
                + "update CodegenConstants in the same commit as the rename/move.",
            Releasable.class.getName(),
            CodegenConstants.RELEASABLE_FQN
        );
    }
}
