/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.CloseableIterator;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Reads data objects in a specific format, producing ESQL Page batches.
 * The output is ESQL's native Page format rather than Arrow to avoid
 * mandating Arrow as a dependency for all format implementations.
 */
public interface FormatReader extends Closeable {

    /** Returns the schema of the data object. */
    List<Attribute> getSchema(StorageObject object) throws IOException;

    /** Opens a reader for the object with optional column projection. Null projectedColumns means all columns. */
    CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) throws IOException;

    /** Returns the format name (e.g., "parquet", "csv", "orc"). */
    String formatName();

    /** Returns file extensions this reader handles (e.g., [".parquet", ".parq"]). */
    List<String> fileExtensions();
}
