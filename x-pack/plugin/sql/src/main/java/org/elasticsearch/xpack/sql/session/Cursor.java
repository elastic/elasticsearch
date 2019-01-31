/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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
    Cursor EMPTY = EmptyCursor.INSTANCE;

    /**
     * Request the next page of data.
     */
    void nextPage(Configuration cfg, Client client, NamedWriteableRegistry registry, ActionListener<RowSet> listener);

    /**
     *  Cleans the resources associated with the cursor
     */
    void clear(Configuration cfg, Client client, ActionListener<Boolean> listener);
}