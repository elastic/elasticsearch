/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.apache.hadoop.thirdparty.com.google.common.collect;

/**
 * Minimal stub replacing the {@code hadoop-shaded-guava} jar (3.5 MB).
 * <p>
 * The original interface lives in Google Guava ({@code com.google.common.collect.Interner},
 * Apache License 2.0) and is shaded by the Hadoop project into the
 * {@code org.apache.hadoop.thirdparty} package namespace.
 * {@code org.apache.hadoop.util.StringInterner} in {@code hadoop-common}
 * holds a field of this type, populated via {@link Interners#newStrongInterner()}.
 * <p>
 * This is a single-method interface whose signature is dictated by the calling
 * bytecode in {@code hadoop-common}; no code was copied from Guava.
 */
public interface Interner<E> {
    E intern(E sample);
}
