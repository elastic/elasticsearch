/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.session;

import java.util.function.Consumer;

/**
 * A set of rows to be returned at one time and a way
 * to get the next set of rows.
 */
public interface RowSet extends RowView {

    boolean hasCurrentRow();

    boolean advanceRow();

    // number or rows in this set; while not really necessary (the return of advanceRow works)
    int size();

    void reset();

    default void forEachRow(Consumer<? super RowView> action) {
        for (boolean hasRows = hasCurrentRow(); hasRows; hasRows = advanceRow()) {
            action.accept(this);
        }
    }
}
