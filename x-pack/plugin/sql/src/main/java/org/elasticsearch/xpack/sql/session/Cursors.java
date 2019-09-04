/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.Version;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.common.io.SqlStreamInput;
import org.elasticsearch.xpack.sql.common.io.SqlStreamOutput;
import org.elasticsearch.xpack.sql.execution.search.CompositeAggregationCursor;
import org.elasticsearch.xpack.sql.execution.search.ScrollCursor;
import org.elasticsearch.xpack.sql.execution.search.extractor.BucketExtractors;
import org.elasticsearch.xpack.sql.execution.search.extractor.HitExtractors;
import org.elasticsearch.xpack.sql.expression.function.scalar.Processors;
import org.elasticsearch.xpack.sql.expression.literal.Literals;
import org.elasticsearch.xpack.sql.plugin.TextFormatterCursor;
import org.elasticsearch.xpack.sql.util.StringUtils;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/**
 * Registry and utilities around {@link Cursor}s.
 */
public final class Cursors {

    private static final NamedWriteableRegistry WRITEABLE_REGISTRY = new NamedWriteableRegistry(getNamedWriteables());
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
        entries.add(new NamedWriteableRegistry.Entry(Cursor.class, CompositeAggregationCursor.NAME, CompositeAggregationCursor::new));
        entries.add(new NamedWriteableRegistry.Entry(Cursor.class, TextFormatterCursor.NAME, TextFormatterCursor::new));
        entries.add(new NamedWriteableRegistry.Entry(Cursor.class, ListCursor.NAME, ListCursor::new));

        // plus all their dependencies
        entries.addAll(Processors.getNamedWriteables());
        entries.addAll(HitExtractors.getNamedWriteables());
        entries.addAll(BucketExtractors.getNamedWriteables());

        // and custom types
        entries.addAll(Literals.getNamedWriteables());

        return entries;
    }

    /**
     * Write a {@linkplain Cursor} to a string for serialization across xcontent.
     */
    public static String encodeToString(Cursor info, ZoneId zoneId) {
        return encodeToString(info, VERSION, zoneId);
    }

    static String encodeToString(Cursor info, Version version, ZoneId zoneId) {
        if (info == Cursor.EMPTY) {
            return StringUtils.EMPTY;
        }
        try (SqlStreamOutput output = new SqlStreamOutput(version, zoneId)) {
            output.writeNamedWriteable(info);
            output.close();
            // return the string only after closing the resource
            return output.streamAsString();
        } catch (IOException ex) {
            throw new SqlIllegalArgumentException("Unexpected failure retrieving next page", ex);
        }
    }


    /**
     * Read a {@linkplain Cursor} from a string.
     */
    public static Cursor decodeFromString(String base64) {
        return decodeFromStringWithZone(base64).v1();
    }

    /**
     * Read a {@linkplain Cursor} from a string.
     */
    public static Tuple<Cursor, ZoneId> decodeFromStringWithZone(String base64) {
        if (base64.isEmpty()) {
            return new Tuple<>(Cursor.EMPTY, null);
        }
        try (SqlStreamInput in = new SqlStreamInput(base64, WRITEABLE_REGISTRY, VERSION)) {
            Cursor cursor = in.readNamedWriteable(Cursor.class);
            return new Tuple<>(cursor, in.zoneId());
        } catch (IOException ex) {
            throw new SqlIllegalArgumentException("Unexpected failure decoding cursor", ex);
        }
    }

}
