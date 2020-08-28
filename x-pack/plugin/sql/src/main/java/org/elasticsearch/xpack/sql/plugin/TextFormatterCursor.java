/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.action.BasicFormatter;
import org.elasticsearch.xpack.sql.session.SqlConfiguration;
import org.elasticsearch.xpack.sql.session.Cursor;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ActionListener.wrap;
/**
 * The cursor that wraps all necessary information for textual representation of the result table
 */
public class TextFormatterCursor implements Cursor {
    public static final String NAME = "f";

    private final Cursor delegate;
    private final BasicFormatter formatter;

    TextFormatterCursor(Cursor delegate, BasicFormatter formatter) {
        this.delegate = delegate;
        this.formatter = formatter;
    }

    public TextFormatterCursor(StreamInput in) throws IOException {
        delegate = in.readNamedWriteable(Cursor.class);
        formatter = new BasicFormatter(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(delegate);
        formatter.writeTo(out);
    }

    public BasicFormatter getFormatter() {
        return formatter;
    }

    @Override
    public void nextPage(SqlConfiguration cfg, Client client, NamedWriteableRegistry registry, ActionListener<Page> listener) {
        // keep wrapping the text formatter
        delegate.nextPage(cfg, client, registry,
                wrap(p -> {
                    Cursor next = p.next();
                    listener.onResponse(next == Cursor.EMPTY ? p : new Page(p.rowSet(), new TextFormatterCursor(next, formatter)));
                }, listener::onFailure));
    }

    @Override
    public void clear(SqlConfiguration cfg, Client client, ActionListener<Boolean> listener) {
        delegate.clear(cfg, client, listener);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TextFormatterCursor that = (TextFormatterCursor) o;
        return Objects.equals(delegate, that.delegate) &&
                Objects.equals(formatter, that.formatter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(delegate, formatter);
    }
}
