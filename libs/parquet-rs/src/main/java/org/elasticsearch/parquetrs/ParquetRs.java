/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.parquetrs;

import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.ParquetRsFunctions;

import java.util.Optional;

/**
 * Entry point for the parquet-rs native library. Provides access to the
 * underlying {@link ParquetRsFunctions} via {@link NativeAccess}.
 */
public final class ParquetRs {

    private ParquetRs() {}

    /**
     * Returns the native parquet-rs functions, or an empty optional if the
     * native library is not available on this platform.
     */
    public static Optional<ParquetRsFunctions> functions() {
        return NativeAccess.instance().getParquetRsFunctions();
    }
}
