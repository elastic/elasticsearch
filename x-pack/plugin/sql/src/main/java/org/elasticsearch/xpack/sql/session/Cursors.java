/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.execution.search.CompositeAggregationCursor;
import org.elasticsearch.xpack.sql.execution.search.ScrollCursor;
import org.elasticsearch.xpack.sql.execution.search.extractor.BucketExtractors;
import org.elasticsearch.xpack.sql.execution.search.extractor.HitExtractors;
import org.elasticsearch.xpack.sql.expression.function.scalar.Processors;
import org.elasticsearch.xpack.sql.expression.literal.Intervals;
import org.elasticsearch.xpack.sql.plugin.CliFormatterCursor;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Registry and utilities around {@link Cursor}s.
 */
public final class Cursors {

    private static final NamedWriteableRegistry WRITEABLE_REGISTRY = new NamedWriteableRegistry(getNamedWriteables());

    private Cursors() {};

    /**
     * The {@link NamedWriteable}s required to deserialize {@link Cursor}s.
     */
    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();

        // cursors
        entries.add(new NamedWriteableRegistry.Entry(Cursor.class, EmptyCursor.NAME, in -> Cursor.EMPTY));
        entries.add(new NamedWriteableRegistry.Entry(Cursor.class, ScrollCursor.NAME, ScrollCursor::new));
        entries.add(new NamedWriteableRegistry.Entry(Cursor.class, CompositeAggregationCursor.NAME, CompositeAggregationCursor::new));
        entries.add(new NamedWriteableRegistry.Entry(Cursor.class, CliFormatterCursor.NAME, CliFormatterCursor::new));

        // plus all their dependencies
        entries.addAll(Processors.getNamedWriteables());
        entries.addAll(HitExtractors.getNamedWriteables());
        entries.addAll(BucketExtractors.getNamedWriteables());

        // and custom types
        entries.addAll(Intervals.getNamedWriteables());

        return entries;
    }

    /**
     * Write a {@linkplain Cursor} to a string for serialization across xcontent.
     */
    public static String encodeToString(Version version, Cursor info) {
        if (info == Cursor.EMPTY) {
            return "";
        }
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            try (OutputStream base64 = Base64.getEncoder().wrap(os); StreamOutput out = new OutputStreamStreamOutput(base64)) {
                Version.writeVersion(version, out);
                out.writeNamedWriteable(info);
            }
            return os.toString(StandardCharsets.UTF_8.name());
        } catch (Exception ex) {
            throw new SqlIllegalArgumentException("Unexpected failure retriving next page", ex);
        }
    }


    /**
     * Read a {@linkplain Cursor} from a string.
     */
    public static Cursor decodeFromString(String info) {
        if (info.isEmpty()) {
            return Cursor.EMPTY;
        }
        byte[] bytes = info.getBytes(StandardCharsets.UTF_8);
        try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(Base64.getDecoder().decode(bytes)), WRITEABLE_REGISTRY)) {
            Version version = Version.readVersion(in);
            if (version.after(Version.CURRENT)) {
                throw new SqlIllegalArgumentException("Unsupported cursor version " + version);
            }
            in.setVersion(version);
            return in.readNamedWriteable(Cursor.class);
        } catch (SqlIllegalArgumentException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new SqlIllegalArgumentException("Unexpected failure decoding cursor", ex);
        }
    }
}