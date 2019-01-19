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
import org.elasticsearch.xpack.sql.action.CliFormatter;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.elasticsearch.xpack.sql.session.RowSet;

import java.io.IOException;
import java.util.Objects;

/**
 * The cursor that wraps all necessary information for textual representation of the result table
 */
public class CliFormatterCursor implements Cursor {
    public static final String NAME = "f";

    private final Cursor delegate;
    private final CliFormatter formatter;

    /**
     * If the newCursor is empty, returns an empty cursor. Otherwise, creates a new
     * CliFormatterCursor that wraps the newCursor.
     */
    public static Cursor wrap(Cursor newCursor, CliFormatter formatter) {
        if (newCursor == EMPTY) {
            return EMPTY;
        }
        return new CliFormatterCursor(newCursor, formatter);
    }

    private CliFormatterCursor(Cursor delegate, CliFormatter formatter) {
        this.delegate = delegate;
        this.formatter = formatter;
    }

    public CliFormatterCursor(StreamInput in) throws IOException {
        delegate = in.readNamedWriteable(Cursor.class);
        formatter = new CliFormatter(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(delegate);
        formatter.writeTo(out);
    }

    public CliFormatter getCliFormatter() {
        return formatter;
    }

    @Override
    public void nextPage(Configuration cfg, Client client, NamedWriteableRegistry registry, ActionListener<RowSet> listener) {
        delegate.nextPage(cfg, client, registry, listener);
    }

    @Override
    public void clear(Configuration cfg, Client client, ActionListener<Boolean> listener) {
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
        CliFormatterCursor that = (CliFormatterCursor) o;
        return Objects.equals(delegate, that.delegate) &&
                Objects.equals(formatter, that.formatter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(delegate, formatter);
    }
}
