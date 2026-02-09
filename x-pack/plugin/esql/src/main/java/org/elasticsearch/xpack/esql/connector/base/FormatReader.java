/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.connector.base;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;

import java.util.List;
import java.util.OptionalLong;

/**
 * SPI for reading a file format.
 *
 * <p>Implementations handle format-specific concerns: schema inference from file
 * metadata, predicate pushdown to format-native filters, columnar or row-based
 * reading, and compression/encoding handling. The format layer is independent of
 * storage — the same {@link FormatReader} works with files on S3, GCS, HDFS, or
 * local filesystem.
 *
 * <p>Example implementations:
 * <ul>
 *   <li>Parquet — read schema from footer, push filters to row group level</li>
 *   <li>ORC — read schema from file metadata, push filters to stripe level</li>
 *   <li>CSV — infer schema from header row, no filter pushdown</li>
 *   <li>Avro — read schema from file header</li>
 * </ul>
 *
 * <p>Filter translation lives in {@link FormatReader} because different formats
 * have different pushdown capabilities. Parquet supports row-group predicates,
 * ORC supports stripe-level predicates, and CSV has no filter pushdown at all.
 *
 * @see StorageProvider
 * @see DataLakeConnector
 */
public interface FormatReader {

    /**
     * The file format identifier.
     *
     * <p>Examples: {@code "parquet"}, {@code "orc"}, {@code "csv"}, {@code "avro"}
     */
    String format();

    /**
     * Infer schema from a file.
     *
     * <p>Reads format-specific metadata (Parquet footer, ORC file metadata, CSV
     * header row) to extract the schema. Only the metadata is read, not the
     * actual data.
     *
     * @param storage Storage provider for opening the file
     * @param file The file to read schema from
     * @return Schema as ES|QL attributes
     */
    List<Attribute> inferSchema(StorageProvider storage, StorageObject file);

    /**
     * Translate an ES|QL filter expression to a format-native filter.
     *
     * <p>The returned {@link DataLakeConnector.FilterTranslation} indicates whether
     * the filter was fully translated, partially translated (with a remainder for
     * ES|QL to evaluate), or not translatable at all.
     *
     * <p>Typical pushdown capabilities by format:
     * <ul>
     *   <li>Parquet — equality, range, IN for row group min/max statistics</li>
     *   <li>ORC — equality, range for stripe-level statistics</li>
     *   <li>CSV — no pushdown (returns {@code FilterTranslation.none()})</li>
     * </ul>
     *
     * @param filter The ES|QL filter expression
     * @return Translation result
     */
    DataLakeConnector.FilterTranslation translateFilter(Expression filter);

    /**
     * Reads batches of rows from a file.
     *
     * <p>Created by the execution phase to stream data from a single file.
     * Implementations handle format-specific reading (Parquet row groups,
     * ORC stripes, CSV lines) and apply any pushed-down filters.
     */
    interface BatchReader extends AutoCloseable {

        /**
         * Read the next batch of rows.
         *
         * @param maxRows Maximum number of rows to read in this batch
         * @return Batch of rows (format depends on compute engine), or null if EOF
         */
        Object nextBatch(int maxRows);

        /**
         * Estimated total rows in this file, if known from metadata.
         */
        OptionalLong estimatedRows();

        @Override
        void close();
    }
}
