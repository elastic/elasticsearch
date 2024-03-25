/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.esql.action;

import org.elasticsearch.core.Releasable;

import java.util.List;

/**
 * An ES|QL Response object.
 *
 * <p> Iterator based access to values of type T has the following properties:
 * <ol>
 *   <li>single-value is of type {@code T}</li>
 *   <li>multi-value is of type {@code List<T>}</li>
 *   <li>absent value is {@code null}</li>
 * </ol>
 *
 * <p> This response object should be closed when the consumer of its values
 * is finished. Closing the response object invalidates any iterators of its
 * values. An invalidated iterator, if not already exhausted, will eventually
 * throw an IllegalStateException. Once a response object is closed, calling
 * {@link #rows()}, {@link #column(int)}, or operating on an Iterable return
 * from the aforementioned value accessor methods, results in an
 * IllegalStateException.
 */
public interface EsqlResponse extends Releasable {

    /** Returns the column info. */
    List<? extends ColumnInfo> columns();

    /**
     * Returns an iterable that allows to iterator over the values in all rows
     * of the response, this is the rows-iterator. A further iterator can be
     * retrieved from the rows-iterator, which iterates over the actual values
     * in the row, one row at a time, column-wise from left to right.
     */
    Iterable<Iterable<Object>> rows();

    /** Returns an iterable over the values in the given column. */
    Iterable<Object> column(int columnIndex);
}
