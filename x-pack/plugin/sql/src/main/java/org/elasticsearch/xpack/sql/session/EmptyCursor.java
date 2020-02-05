/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.io.IOException;

class EmptyCursor implements Cursor {
    static final String NAME = "0";
    static final EmptyCursor INSTANCE = new EmptyCursor();

    private EmptyCursor() {
        // Only one instance allowed
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Nothing to write
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void nextPage(Configuration cfg, Client client, NamedWriteableRegistry registry, ActionListener<Page> listener) {
        throw new SqlIllegalArgumentException("there is no next page");
    }

    @Override
    public void clear(Configuration cfg, Client client, ActionListener<Boolean> listener) {
        // There is nothing to clean
        listener.onResponse(false);
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this;
    }

    @Override
    public int hashCode() {
        return 27;
    }

    @Override
    public String toString() {
        return "no next page";
    }
}
