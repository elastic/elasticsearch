/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import java.io.IOException;

/** Opens the Parquet files that segments reference. A segment stores where its files are and never how to reach them. */
@FunctionalInterface
public interface ParquetObjects {

    /**
     * Opens the file at the stored location. This must not make a request when the length is known, because opening a shard must not
     * touch the object store.
     *
     * @param location the storage reference that the segment stores
     * @param length the length of the file when it was attached
     */
    ParquetObject open(String location, long length) throws IOException;
}
