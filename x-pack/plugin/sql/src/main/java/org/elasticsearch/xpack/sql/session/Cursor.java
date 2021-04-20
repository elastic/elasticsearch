/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;

/**
 * Information required to access the next page of response.
 */
public interface Cursor extends NamedWriteable {

    class Page {
        private final RowSet rowSet;
        private final Cursor next;

        public Page(RowSet rowSet, Cursor next) {
            this.rowSet = rowSet;
            this.next = next;
        }

        public RowSet rowSet() {
            return rowSet;
        }

        public Cursor next() {
            return next;
        }

        public static Page last(RowSet rowSet) {
            return new Page(rowSet, EMPTY);
        }
    }

    Cursor EMPTY = EmptyCursor.INSTANCE;

    /**
     * Request the next page of data.
     */
    void nextPage(SqlConfiguration cfg, Client client, NamedWriteableRegistry registry, ActionListener<Page> listener);

    /**
     *  Cleans the resources associated with the cursor
     */
    void clear(SqlConfiguration cfg, Client client, ActionListener<Boolean> listener);
}
