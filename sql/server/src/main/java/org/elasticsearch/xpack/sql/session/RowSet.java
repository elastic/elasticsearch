/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.sql.execution.PlanExecutor;

import java.util.function.Consumer;

/**
 * A set of rows to be returned at one time and a way
 * to get the next set of rows.
 */
public interface RowSet extends RowView {

    boolean hasCurrentRow();

    boolean advanceRow();

    int size(); // NOCOMMIT why do we have this? It looks like the count of the rows in this chunk.

    void reset();

    /**
     * The key used by {@link PlanExecutor#nextPage(Cursor, ActionListener)} to fetch the next page.
     */
    Cursor nextPageCursor();

    default void forEachRow(Consumer<? super RowView> action) {
        for (boolean hasRows = hasCurrentRow(); hasRows; hasRows = advanceRow()) {
            action.accept(this);
        }
    }
}
