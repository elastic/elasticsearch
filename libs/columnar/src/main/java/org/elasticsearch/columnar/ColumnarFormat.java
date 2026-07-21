/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

/**
 * Format-wide identity and versioning constants for ColumNAR.
 *
 * <p>The version stamp is written once per segment. Writers always emit {@link #VERSION_CURRENT};
 * readers accept any version in {@code [VERSION_START, VERSION_CURRENT]} and branch on it.
 */
public final class ColumnarFormat {

    /** Human-readable format name, used for the codec header and SPI registration. */
    public static final String NAME = "ColumNAR";

    /** First on-disk version. Never reuse or renumber shipped versions. */
    public static final int VERSION_START = 0;

    /** Version written by the current code. Bump when the on-disk layout gains new behavior. */
    public static final int VERSION_CURRENT = VERSION_START;

    private ColumnarFormat() {}
}
