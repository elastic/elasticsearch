/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.ql.util.StringUtils;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.common.io.SqlStreamInput;
import org.elasticsearch.xpack.sql.common.io.SqlStreamOutput;
import org.elasticsearch.xpack.sql.execution.search.CompositeAggCursor;
import org.elasticsearch.xpack.sql.execution.search.PivotCursor;
import org.elasticsearch.xpack.sql.execution.search.ScrollCursor;
import org.elasticsearch.xpack.sql.execution.search.extractor.SqlBucketExtractors;
import org.elasticsearch.xpack.sql.execution.search.extractor.SqlHitExtractors;
import org.elasticsearch.xpack.sql.expression.function.scalar.Processors;
import org.elasticsearch.xpack.sql.expression.literal.Literals;
import org.elasticsearch.xpack.sql.plugin.FormatterState;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

/**
 * Registry and utilities around {@link Cursor}s.
 */
public final class Cursors {

    private static final Version VERSION = Version.CURRENT;

    private Cursors() {}

    /**
     * The {@link NamedWriteable}s required to deserialize {@link Cursor}s.
     */
    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();

        // cursors
        entries.add(new NamedWriteableRegistry.Entry(Cursor.class, EmptyCursor.NAME, in -> Cursor.EMPTY));
        entries.add(new NamedWriteableRegistry.Entry(Cursor.class, ScrollCursor.NAME, ScrollCursor::new));
        entries.add(new NamedWriteableRegistry.Entry(Cursor.class, CompositeAggCursor.NAME, CompositeAggCursor::new));
        entries.add(new NamedWriteableRegistry.Entry(Cursor.class, PivotCursor.NAME, PivotCursor::new));
        entries.add(new NamedWriteableRegistry.Entry(Cursor.class, ListCursor.NAME, ListCursor::new));

        // plus all their dependencies
        entries.addAll(Processors.getNamedWriteables());
        entries.addAll(SqlHitExtractors.getNamedWriteables());
        entries.addAll(SqlBucketExtractors.getNamedWriteables());

        // and custom types
        entries.addAll(Literals.getNamedWriteables());

        entries.addAll(FormatterState.getNamedWriteables());

        return entries;
    }

    /**
     * Write a {@linkplain Cursor} to a string for serialization across xcontent.
     */
    public static String encodeToString(Cursor info, ZoneId zoneId) {
        return encodeToString(info, VERSION, zoneId);
    }

    public static String encodeToString(Cursor info, Version version, ZoneId zoneId) {
        if (info == Cursor.EMPTY) {
            return StringUtils.EMPTY;
        }
        try (SqlStreamOutput output = SqlStreamOutput.create(version, zoneId)) {
            output.writeOptionalNamedWriteable(null);
            output.writeNamedWriteable(info);
            output.close();
            // return the string only after closing the resource
            return output.streamAsString();
        } catch (IOException ex) {
            throw new SqlIllegalArgumentException("Unexpected failure writing cursor", ex);
        }
    }

    public static String attachState(String cursor, FormatterState state) {
        if (Strings.isNullOrEmpty(cursor) || state == null) {
            return cursor;
        } else {
            try (SqlStreamOutput output = SqlStreamOutput.create(VERSION, ZoneOffset.UTC)) {
                output.writeOptionalNamedWriteable(state);
                output.writeString(cursor);
                output.close();
                // return the string only after closing the resource
                return output.streamAsString();
            } catch (IOException ex) {
                throw new SqlIllegalArgumentException("Unexpected failure writing cursor", ex);
            }
        }
    }

    private static final NamedWriteableRegistry FORMATTER_STATE_REGISTRY = new NamedWriteableRegistry(FormatterState.getNamedWriteables());

    public static FormatterState decodeState(String base64) {
        if (base64.isEmpty()) {
            return null;
        }
        try (SqlStreamInput in = SqlStreamInput.fromString(base64, FORMATTER_STATE_REGISTRY, VERSION)) {
            return in.readOptionalNamedWriteable(FormatterState.class);
        } catch (IOException ex) {
            throw new SqlIllegalArgumentException("Unexpected failure reading cursor", ex);
        }
    }

    /**
     * Read a {@linkplain Cursor} from a string.
     */
    public static Tuple<Cursor, ZoneId> decodeFromStringWithZone(String base64, NamedWriteableRegistry writeableRegistry) {
        if (base64.isEmpty()) {
            return new Tuple<>(Cursor.EMPTY, null);
        }
        try (SqlStreamInput in = SqlStreamInput.fromString(base64, writeableRegistry, VERSION)) {
            if (in.readOptionalNamedWriteable(FormatterState.class) == null) {
                Cursor cursor = in.readNamedWriteable(Cursor.class);
                return new Tuple<>(cursor, in.zoneId());
            } else {
                return decodeFromStringWithZone(in.readString(), writeableRegistry);
            }
        } catch (IOException ex) {
            throw new SqlIllegalArgumentException("Unexpected failure reading cursor", ex);
        }
    }

}
