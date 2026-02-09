/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.connector.base;

import java.util.List;

/**
 * SPI for accessing files in a storage system.
 *
 * <p>Implementations handle storage-specific concerns: authentication, connection
 * pooling, path resolution, and file listing. The storage layer is independent
 * of the file format — the same {@link StorageProvider} works with Parquet, ORC,
 * CSV, or any other format read by a {@link FormatReader}.
 *
 * <p>Example implementations:
 * <ul>
 *   <li>S3 — list and read objects from Amazon S3 buckets</li>
 *   <li>GCS — list and read objects from Google Cloud Storage</li>
 *   <li>HDFS — list and read files from Hadoop Distributed File System</li>
 *   <li>Local FS — list and read files from local filesystem</li>
 * </ul>
 *
 * @see StorageObject
 * @see FormatReader
 * @see DataLakeConnector
 */
public interface StorageProvider {

    /**
     * The storage system type identifier.
     *
     * <p>Examples: {@code "s3"}, {@code "gcs"}, {@code "hdfs"}, {@code "file"}
     */
    String type();

    /**
     * List storage objects matching a pattern.
     *
     * <p>The pattern is storage-specific. Examples:
     * <ul>
     *   <li>S3: {@code "logs/*.parquet"} — prefix + glob within a bucket</li>
     *   <li>Local: {@code "/data/logs/*.parquet"} — filesystem glob</li>
     * </ul>
     *
     * @param pattern Storage-specific pattern for matching files
     * @return List of matching storage objects
     */
    List<StorageObject> listObjects(String pattern);

    /**
     * Open a storage object for reading.
     *
     * @param object The object to open
     * @return A read handle for accessing the object's contents
     */
    StorageObject.ReadHandle open(StorageObject object);
}
