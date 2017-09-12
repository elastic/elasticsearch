/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.xpack.sql.execution.search.ScrollCursor;
import org.elasticsearch.xpack.sql.execution.search.extractor.HitExtractors;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Information required to access the next page of response.
 */
public interface Cursor extends NamedWriteable {
    Cursor EMPTY = EmptyCursor.INSTANCE;

    /**
     * Request the next page of data.
     */
    void nextPage(Client client, ActionListener<RowSetCursor> listener);
    /**
     * Write the {@linkplain Cursor} to a String for serialization over xcontent.
     */
    void writeTo(java.io.Writer writer) throws IOException;

    /**
     * The {@link NamedWriteable}s required to deserialize {@link Cursor}s.
     */
    static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(HitExtractors.getNamedWriteables());
        entries.add(new NamedWriteableRegistry.Entry(Cursor.class, EmptyCursor.NAME, in -> EMPTY));
        entries.add(new NamedWriteableRegistry.Entry(Cursor.class, ScrollCursor.NAME, ScrollCursor::new));
        return entries;
    }

    /**
     * Write a {@linkplain Cursor} to a string for serialization across xcontent.
     */
    static String encodeToString(Cursor info) {
        StringWriter writer = new StringWriter();
        try {
            writer.write(info.getWriteableName());
            info.writeTo(writer);
        } catch (IOException e) {
            throw new RuntimeException("unexpected failure converting next page info to a string", e);
        }
        return writer.toString();
    }

    /**
     * Read a {@linkplain Cursor} from a string.
     */
    static Cursor decodeFromString(String info) {
        // TODO version compatibility
        /* We need to encode minimum version across the cluster and use that
         * to handle changes to this protocol across versions. */
        String name = info.substring(0, 1);
        try (java.io.Reader reader = new FastStringReader(info)) {
            reader.skip(1);
            switch (name) {
            case EmptyCursor.NAME:
                throw new RuntimeException("empty cursor shouldn't be encoded to a string");
            case ScrollCursor.NAME:
                return new ScrollCursor(reader);
            default:
                throw new RuntimeException("unknown cursor type [" + name + "]");
            }
        } catch (IOException e) {
            throw new RuntimeException("unexpected failure deconding cursor", e);
        }
    }
}