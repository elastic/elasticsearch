/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.apache.commons.lang3;

/**
 * Minimal stub replacing the {@code commons-lang3} jar.
 * <p>
 * The original class lives in Apache Commons Lang
 * ({@code org.apache.commons.lang3.StringUtils}, Apache License 2.0). The
 * bytecode of {@code org.apache.hadoop.fs.Path#normalizePath} (in
 * {@code hadoop-common}) calls {@code StringUtils.replace(path, "\\", "/")}
 * to convert backslashes to forward slashes — but only inside an
 * {@code if (Path.WINDOWS)} branch (further gated on a Windows drive prefix
 * or {@code file:}/empty scheme). On Linux/macOS the JVM never resolves this
 * reference, so the missing class stays latent; on Windows, the first
 * {@code file:///C:/…} path passed to {@link org.apache.hadoop.fs.Path#Path}
 * triggers class-loading and crashes the node with
 * {@code NoClassDefFoundError}.
 * <p>
 * Hadoop also references {@code StringUtils} from its
 * {@code IOStatisticsContext → Shell.<clinit>} chain (documented in
 * {@code OrcFormatReaderTests.NoPermissionLocalFileSystem}), but that chain
 * is only reachable through {@code LocalFSFileOutputStream} on the write
 * path, which the production reader never exercises. Providing the single
 * {@link #replace} method is therefore sufficient.
 * <p>
 * This is an independent reimplementation of the documented contract; no
 * code was copied from Apache Commons Lang.
 */
public final class StringUtils {

    private StringUtils() {}

    /**
     * Replaces all occurrences of {@code searchString} in {@code text} with
     * {@code replacement}. Matches the contract of
     * {@code org.apache.commons.lang3.StringUtils#replace(String, String, String)}:
     * a {@code null} {@code text}/{@code searchString}/{@code replacement} or
     * an empty {@code searchString} returns {@code text} unchanged; otherwise
     * delegates to {@link String#replace(CharSequence, CharSequence)} for
     * literal (non-regex) replacement.
     */
    public static String replace(String text, String searchString, String replacement) {
        if (text == null || searchString == null || replacement == null || searchString.isEmpty()) {
            return text;
        }
        return text.replace(searchString, replacement);
    }
}
