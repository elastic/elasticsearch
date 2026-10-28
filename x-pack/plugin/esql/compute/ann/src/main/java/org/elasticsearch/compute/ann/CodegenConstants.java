/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.ann;

/**
 * Fully-qualified type names that the annotation processor needs to reference as
 * strings. Annotation processors run during compilation and operate on
 * {@code javax.lang.model.TypeMirror} (not {@code java.lang.Class}), so they
 * cannot use {@code Foo.class.getName()} for types they're processing — only the
 * FQN string. To keep this from drifting silently when types are renamed/moved,
 * each entry here is backed by a runtime-checked test
 * ({@code CodegenConstantsContractTests}) that asserts
 * {@code Foo.class.getName().equals(FOO_FQN)}.
 *
 * <p>This lives in the {@code compute/ann} module so both the annotation processor
 * ({@code compute/gen}) and runtime tests ({@code compute/src/test}) can see it.
 */
public final class CodegenConstants {
    private CodegenConstants() {}

    /** FQN of {@code org.elasticsearch.core.Releasable}. */
    public static final String RELEASABLE_FQN = "org.elasticsearch.core.Releasable";
}
