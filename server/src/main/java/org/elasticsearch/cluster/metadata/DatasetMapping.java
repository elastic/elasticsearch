/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * A user-declared mapping attached to a {@link Dataset}. Entirely optional — a dataset with no
 * {@code DatasetMapping} resolves its schema by inference, exactly as before.
 *
 * <p>Currently this wraps a single {@code mappings} block ({@link Mappings}): a {@code dynamic} mode and per-column
 * {@code properties}. The wrapper is retained (rather than inlining {@code mappings} onto {@link Dataset}) so future
 * top-level declaration keys have a home.
 *
 * <p>There are <b>no role designations</b>. A time axis is just a column named {@code @timestamp}, declared as an
 * ordinary rename ({@code "@timestamp": {"type":"date","path":"ts"}}) and recognized by the stack by name — a
 * "move", not a designation. Whether the named column exists is validated in the ES|QL layer: at put time when it
 * is declared, otherwise at first query.
 *
 * <p>Like {@link DataSourceReference}, this has no standalone XContent: {@link Dataset#toXContent} emits the
 * {@code mappings} key and {@link Dataset#PARSER} reads it back, assembling this object via {@link #assemble}.
 * That keeps a single on-disk JSON shape.
 */
public final class DatasetMapping implements Writeable {

    /** Undeclared-column policy. Mirrors Elasticsearch {@code mappings.dynamic}; only the two read-applicable values. */
    public enum Dynamic {
        /** Inference fills columns not named in {@code properties} (non-strict). The default when a mappings block is present. */
        TRUE,
        /** The declaration is the entire schema; no inference, undeclared columns are not queryable (strict). */
        FALSE;

        public static Dynamic fromString(String value) {
            return switch (value.toLowerCase(Locale.ROOT)) {
                case "true" -> TRUE;
                case "false" -> FALSE;
                default -> throw new IllegalArgumentException("unknown dynamic value [" + value + "]; supported values are [true, false]");
            };
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    /**
     * The {@code mappings} block: an undeclared-column policy and the per-column declarations keyed by logical name.
     *
     * @param dynamic    undeclared-column policy ({@code true} = infer + overlay, {@code false} = declaration is the
     *                   whole schema).
     * @param properties per-column declarations keyed by logical name; order-preserving, may be empty (e.g.
     *                   {@code "mappings": { "dynamic": "false" }}).
     */
    public record Mappings(Dynamic dynamic, Map<String, DatasetFieldMapping> properties) implements Writeable {

        public Mappings {
            Objects.requireNonNull(dynamic, "dynamic must not be null");
            properties = properties == null ? Map.of() : Collections.unmodifiableMap(properties);
        }

        Mappings(StreamInput in) throws IOException {
            this(in.readEnum(Dynamic.class), in.readOrderedMap(StreamInput::readString, DatasetFieldMapping::new));
            // An optional string this version has no field for. dataset_declared_schema is on 9.5, so dropping the
            // read would be a wire break; a 9.5 peer writes a column name here and it is discarded.
            // TODO: remove the slot once 9.5 is out of the wire-compatibility window.
            in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(dynamic);
            out.writeMap(properties, (o, v) -> v.writeTo(o));
            out.writeOptionalString(null); // the _id.path slot a 9.5 peer expects; see the stream constructor
        }
    }

    private static final String DYNAMIC = "dynamic";
    private static final String PROPERTIES = "properties";
    /** Not a field this version has; {@link #parseStoredMappings} skips it in state a 9.5 node persisted. */
    // TODO: remove this and the tolerant entry point once no supported upgrade starts from a node that writes it.
    private static final String UNSUPPORTED_ID_FIELD = "_id";

    @Nullable
    private final Mappings mappings;

    public DatasetMapping(@Nullable Mappings mappings) {
        this.mappings = mappings;
    }

    public DatasetMapping(StreamInput in) throws IOException {
        this.mappings = in.readOptionalWriteable(Mappings::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(mappings);
    }

    /**
     * Builds a {@link DatasetMapping} from the parsed {@code mappings} block, or {@code null} when it is absent (a
     * dataset with no declared schema). Used by {@link Dataset#PARSER}. Every declaration surface lives inside
     * {@code mappings}, so a dataset that only sets, say, {@code dynamic} still needs a {@code mappings} wrapper.
     */
    @Nullable
    public static DatasetMapping assemble(@Nullable Mappings mappings) {
        return mappings == null ? null : new DatasetMapping(mappings);
    }

    /**
     * Parses a user-supplied {@code mappings} object ({@code dynamic}, {@code properties}). A {@code _id} block is
     * not a field this version has, and is refused like any other unknown key — a dataset answers
     * {@code METADATA _id} as SQL NULL, so accepting the declaration would take a column name and do nothing with it.
     */
    public static Mappings parseMappings(XContentParser parser) throws IOException {
        return parseMappings(parser, false);
    }

    /**
     * Parses a {@code mappings} object off persisted cluster state, where a 9.5 node may have written a
     * {@code _id} block. That block is read and discarded: refusing it would leave an upgraded node unable to
     * load its own cluster state, and there is nothing for the declaration to feed. Precedent for skipping an
     * unsupported block unexamined is {@code IndexMetadata.Builder.fromXContent}'s {@code warmers} arm.
     * ({@link DataStream}'s {@code timestamp_field} is a different shape — it validates and writes the block back.)
     */
    public static Mappings parseStoredMappings(XContentParser parser) throws IOException {
        return parseMappings(parser, true);
    }

    private static Mappings parseMappings(XContentParser parser, boolean fromStoredState) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        Dynamic dynamic = Dynamic.TRUE;
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        String field = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                field = parser.currentName();
            } else if (DYNAMIC.equals(field)) {
                dynamic = Dynamic.fromString(parser.text());
            } else if (PROPERTIES.equals(field)) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
                String name = null;
                XContentParser.Token t;
                while ((t = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (t == XContentParser.Token.FIELD_NAME) {
                        name = parser.currentName();
                    } else {
                        properties.put(name, DatasetFieldMapping.fromXContent(parser));
                    }
                }
            } else if (fromStoredState && UNSUPPORTED_ID_FIELD.equals(field)) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
                parser.skipChildren();
            } else {
                throw new IllegalArgumentException("unknown mappings field [" + field + "]");
            }
        }
        return new Mappings(dynamic, properties);
    }

    /** Emits the {@code mappings} block into an open dataset object. */
    public void toXContentFragment(XContentBuilder builder) throws IOException {
        if (mappings != null) {
            builder.startObject("mappings");
            builder.field(DYNAMIC, mappings.dynamic().toString());
            if (mappings.properties().isEmpty() == false) {
                builder.startObject(PROPERTIES);
                for (Map.Entry<String, DatasetFieldMapping> e : mappings.properties().entrySet()) {
                    builder.field(e.getKey());
                    e.getValue().toXContent(builder, null);
                }
                builder.endObject();
            }
            builder.endObject();
        }
    }

    @Nullable
    public Mappings mappings() {
        return mappings;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DatasetMapping that = (DatasetMapping) o;
        return Objects.equals(mappings, that.mappings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mappings);
    }

    @Override
    public String toString() {
        return "DatasetMapping[mappings=" + mappings + "]";
    }
}
