/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.action.BasicFormatter;
import org.elasticsearch.xpack.sql.common.io.SqlStreamInput;
import org.elasticsearch.xpack.sql.common.io.SqlStreamOutput;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.List;

import static org.elasticsearch.core.Tuple.tuple;

public record RestCursorState(BasicFormatter formatter) implements Writeable {

    public RestCursorState(StreamInput in) throws IOException {
        this(in.<BasicFormatter>readOptionalWriteable(BasicFormatter::new));
    }

    public static RestCursorState EMPTY = new RestCursorState((BasicFormatter) null);

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(formatter);
    }

    public static String encodeCursorWithState(String cursor, RestCursorState state) {
        try {
            // compression is disabled because the payload in `cursor` should already be compressed and the state is currently very
            // lightweight
            SqlStreamOutput out = SqlStreamOutput.create(Version.CURRENT, ZoneOffset.UTC, false);
            out.writeString(cursor);
            state.writeTo(out);
            out.close();
            return out.streamAsString();
        } catch (IOException e) {
            throw new SqlIllegalArgumentException(e, "Cannot encode cursor [{}] with state [{}]", cursor, state);
        }
    }

    public static Tuple<String, RestCursorState> decodeCursorWithState(String cursor) {
        if (Strings.isNullOrEmpty(cursor)) {
            return tuple(cursor, EMPTY);
        } else {
            try (StreamInput in = SqlStreamInput.fromString(cursor, EMPTY_WRITEABLE_REGISTRY, Version.CURRENT, false)) {
                String wrappedCursor = in.readString();
                RestCursorState state = new RestCursorState(in);
                return tuple(wrappedCursor, state);
            } catch (IOException e) {
                throw new SqlIllegalArgumentException(e, "Cannot decode cursor [{}]", cursor);
            }
        }
    }

    private static final NamedWriteableRegistry EMPTY_WRITEABLE_REGISTRY = new NamedWriteableRegistry(List.of());
}
