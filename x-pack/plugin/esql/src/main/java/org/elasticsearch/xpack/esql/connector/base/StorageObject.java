/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.connector.base;

import java.io.InputStream;
import java.util.Map;
import java.util.OptionalLong;

/**
 * A handle to a file or object in a storage system.
 *
 * <p>Provides metadata (path, size, partition values) without loading the file contents.
 * Use {@link StorageProvider#open(StorageObject)} to get a {@link ReadHandle} for reading.
 *
 * <p>Partition values are extracted from the object path for Hive-style partitioning
 * (e.g., {@code year=2024/month=01/data.parquet} yields {@code {year: "2024", month: "01"}}).
 *
 * @see StorageProvider
 */
public interface StorageObject {

    /**
     * Full path or URI of this object.
     *
     * <p>Examples: {@code "s3://bucket/logs/2024/01/data.parquet"},
     * {@code "/data/logs/events.parquet"}
     */
    String path();

    /**
     * Size of this object in bytes, if known.
     */
    OptionalLong size();

    /**
     * Partition values extracted from the object path.
     *
     * <p>For Hive-style partitioned paths like {@code year=2024/month=01/data.parquet},
     * returns {@code {year: "2024", month: "01"}}. Returns an empty map if the path
     * has no partition structure.
     */
    Map<String, String> partitionValues();

    /**
     * A handle for reading the contents of a storage object.
     *
     * <p>Supports both sequential streaming and random-access range reads.
     * Range reads are important for columnar formats like Parquet that read
     * specific byte ranges (row groups, column chunks, footer).
     */
    interface ReadHandle extends AutoCloseable {

        /**
         * Open an input stream for sequential reading of the entire object.
         */
        InputStream openStream();

        /**
         * Open an input stream for reading a byte range.
         *
         * <p>Used by columnar format readers (Parquet, ORC) for reading specific
         * sections of a file without downloading the entire object.
         *
         * @param offset Start position in bytes
         * @param length Number of bytes to read
         * @return Input stream positioned at the offset, limited to length bytes
         */
        InputStream openRange(long offset, long length);

        /**
         * Total size of the object in bytes.
         */
        long size();

        @Override
        void close();
    }
}
