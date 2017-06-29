/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.session;

import java.util.function.Consumer;

/**
 * Interface representing a set of rows produced by the SQL engine. Builds on top of a RowView to _prevent_
 * a view object from being instantiated or returned.
 * In other words to enforce immediate consumption (before moving forward).
 * 
 * If (when) joins and such will be enabled, this interface would have to be retro-fitted
 * to become even more lazy (so that things like number of entries) would not be known.
 */
public interface RowSet extends RowView {

    boolean hasCurrentRow();

    boolean advanceRow();

    int size();

    void reset();

    default void forEachRow(Consumer<? super RowView> action) {
        for (boolean hasRows = hasCurrentRow(); hasRows; hasRows = advanceRow()) {
            action.accept(this);
        }
    }
}