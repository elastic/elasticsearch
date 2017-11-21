/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.execution.search.ScrollCursor;
import org.elasticsearch.xpack.sql.execution.search.extractor.HitExtractors;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.elasticsearch.xpack.sql.plugin.AbstractSqlProtocolRestAction.CURSOR_REGISTRY;

/**
 * Information required to access the next page of response.
 */
public interface Cursor extends NamedWriteable {
    Cursor EMPTY = EmptyCursor.INSTANCE;

    /**
     * Request the next page of data.
     */
    void nextPage(Configuration cfg, Client client, ActionListener<RowSet> listener);

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
    static String encodeToString(Version version, Cursor info) {
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            try (OutputStream base64 = Base64.getEncoder().wrap(os);
                 StreamOutput out = new OutputStreamStreamOutput(base64)) {
                Version.writeVersion(version, out);
                out.writeNamedWriteable(info);
            }
            return os.toString(StandardCharsets.UTF_8.name());
        } catch (IOException ex) {
            throw new RuntimeException("unexpected failure converting next page info to a string", ex);
        }
    }


    /**
     * Read a {@linkplain Cursor} from a string.
     */
    static Cursor decodeFromString(String info) {
        byte[] bytes = info.getBytes(StandardCharsets.UTF_8);
        try (StreamInput delegate = new InputStreamStreamInput(Base64.getDecoder().wrap(new ByteArrayInputStream(bytes)));
             StreamInput in = new NamedWriteableAwareStreamInput(delegate, CURSOR_REGISTRY)) {
            Version version = Version.readVersion(in);
            if (version.after(Version.CURRENT)) {
                throw new RuntimeException("Unsupported scroll version " + version);
            }
            in.setVersion(version);
            return in.readNamedWriteable(Cursor.class);
        } catch (IOException ex) {
            throw new RuntimeException("unexpected failure deconding cursor", ex);
        }
    }
}