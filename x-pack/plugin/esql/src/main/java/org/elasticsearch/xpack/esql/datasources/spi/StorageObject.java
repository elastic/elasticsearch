/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;

/**
 * Represents a readable object in external storage.
 * Provides metadata access and methods to open streams for reading.
 * Uses standard Java InputStream for compatibility with existing Elasticsearch code.
 * Random access is handled via range-based reads (like BlobContainer pattern).
 */
public interface StorageObject {

    /** Opens an input stream for sequential reading from the beginning. */
    InputStream newStream() throws IOException;

    /**
     * Opens an input stream for reading a specific byte range.
     * Critical for columnar formats like Parquet that read specific column chunks.
     * For reading object footers (e.g., Parquet), use: {@code newStream(length() - footerSize, footerSize)}
     */
    InputStream newStream(long position, long length) throws IOException;

    /** Returns the object size in bytes. */
    long length() throws IOException;

    /** Returns the last modification time, or null if not available. */
    Instant lastModified() throws IOException;

    /** Checks if the object exists. */
    boolean exists() throws IOException;

    /** Returns the path of this object. */
    StoragePath path();
}
