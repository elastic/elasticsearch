/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.session;

import java.util.Objects;
import java.util.function.Consumer;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.sql.SqlException;

public interface RowSetCursor extends RowSet {

    boolean hasNextSet();

    void nextSet(ActionListener<RowSetCursor> listener);

    default void forEachSet(Consumer<? super RowSet> action) {
        Objects.requireNonNull(action);

        action.accept(this);
        if (hasNextSet()) {
            nextSet(new ActionListener<RowSetCursor>() {
                @Override
                public void onResponse(RowSetCursor cursor) {
                    forEachSet(action);
                }

                @Override
                public void onFailure(Exception ex) {
                    throw ex instanceof RuntimeException ? (RuntimeException) ex : new SqlException(ex);
                }
            });
        }
    }
}