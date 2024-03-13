/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.esql.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.core.Releasable;

import java.util.Iterator;
import java.util.List;

/**
 * Response to an ES|QL query request.
 *
 * <p> Iterator based access to values of type T has the following properties:
 * <ol>
 *   <li>single-value is of type {@code T}</li>
 *   <li>multi-value is of type {@code List<T>}</li>
 *   <li>absent value is {@code null}</li>
 * </ol>
 */
public abstract class EsqlQueryResponse extends ActionResponse implements Releasable {

    public abstract List<? extends ColumnInfo> columns();

    /**
     * Returns an iterator over the values in all rows of the response. The outer
     * iterator returns an inner row-iterator - one per row. The row-iterator
     * iterates over the actual values in the row, column-wise from left to right.
     */
    public abstract Iterator<Iterator<Object>> rows();

    /**
     * Returns an iterator over the values in the given column.
     */
    public abstract Iterator<Object> column(int columnIndex);
}
