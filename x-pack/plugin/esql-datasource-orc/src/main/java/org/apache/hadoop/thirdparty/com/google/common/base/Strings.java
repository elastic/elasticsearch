/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.apache.hadoop.thirdparty.com.google.common.base;

/**
 * Minimal stub replacing the {@code hadoop-shaded-guava} jar (3.5 MB).
 * <p>
 * The original class lives in Google Guava ({@code com.google.common.base.Strings},
 * Apache License 2.0) and is shaded by the Hadoop project into the
 * {@code org.apache.hadoop.thirdparty} package namespace. The bytecode of
 * {@code org.apache.hadoop.conf.Configuration} (in {@code hadoop-common}) calls
 * {@code Strings.isNullOrEmpty()} from this shaded package. Rather than shipping
 * the entire 3.5 MB shaded-guava jar, this stub provides the single method that
 * {@code Configuration} actually invokes at runtime.
 * <p>
 * This is an independent reimplementation (the only possible implementation of
 * this trivial logic); no code was copied from Guava.
 */
public final class Strings {

    private Strings() {}

    public static boolean isNullOrEmpty(String string) {
        return string == null || string.isEmpty();
    }
}
