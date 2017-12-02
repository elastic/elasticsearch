/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.elasticsearch.xpack.sql.session.RowSet;

import java.io.IOException;
import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.List;

/**
 * The cursor that wraps all necessary information for jdbc
 */
public class JdbcCursor implements Cursor {
    public static final String NAME = "j";
    private Cursor delegate;
    private List<JDBCType> types;


    public static Cursor wrap(Cursor newCursor, List<JDBCType> types) {
        if (newCursor == EMPTY) {
            return EMPTY;
        }
        return new JdbcCursor(newCursor, types);
    }

    public JdbcCursor(Cursor delegate, List<JDBCType> types) {
        this.delegate = delegate;
        this.types = types;
    }

    public JdbcCursor(StreamInput in) throws IOException {
        delegate = in.readNamedWriteable(Cursor.class);
        int size = in.readVInt();
        types = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            types.add(JDBCType.valueOf(in.readVInt()));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(delegate);
        out.writeVInt(types.size());
        for (JDBCType type : types) {
            out.writeVInt(type.getVendorTypeNumber());
        }
    }

    public List<JDBCType> getTypes() {
        return types;
    }

    @Override
    public void nextPage(Configuration cfg, Client client, ActionListener<RowSet> listener) {
        delegate.nextPage(cfg, client, listener);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
