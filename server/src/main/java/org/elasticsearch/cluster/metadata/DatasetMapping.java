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
 * <p>Currently this wraps a single {@code mappings} block ({@link Mappings}): a {@code dynamic} mode, per-column
 * {@code properties}, and the meta-field {@code _id} ({@code path}). The wrapper is retained (rather than inlining
 * {@code mappings} onto {@link Dataset}) so future top-level declaration keys have a home.
 *
 * <p>There are <b>no role designations</b>. A time axis is just a column named {@code @timestamp}, declared as an
 * ordinary rename ({@code "@timestamp": {"type":"date","path":"ts"}}) and recognized by the stack by name — a
 * "move", not a designation. Setting {@code _id} from a column is likewise a meta-field
 * ({@code "_id": {"path": "col"}}) inside {@code mappings} — the ES meta-field shape, not a separate top-level role
 * — so it always rides a {@code mappings} wrapper. Whether the named column exists is validated in the ES|QL layer:
 * at put time when it is declared, otherwise at first query.
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
     * @param idPath     {@code _id.path}: the column the reader stamps {@code _id} from, or {@code null} when unset.
     */
    public record Mappings(Dynamic dynamic, Map<String, DatasetFieldMapping> properties, @Nullable String idPath) implements Writeable {

        public Mappings {
            Objects.requireNonNull(dynamic, "dynamic must not be null");
            properties = properties == null ? Map.of() : Collections.unmodifiableMap(properties);
        }

        /** Convenience: a mappings block with no {@code _id.path}. */
        public Mappings(Dynamic dynamic, Map<String, DatasetFieldMapping> properties) {
            this(dynamic, properties, null);
        }

        Mappings(StreamInput in) throws IOException {
            // The whole DatasetMapping is gated by the dataset_declared_schema transport version (see Dataset), which is
            // unreleased — so every field (incl. _id.path) ships in that one version; no separate gate.
            this(in.readEnum(Dynamic.class), in.readOrderedMap(StreamInput::readString, DatasetFieldMapping::new), in.readOptionalString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(dynamic);
            out.writeMap(properties, (o, v) -> v.writeTo(o));
            out.writeOptionalString(idPath);
        }
    }

    private static final String DYNAMIC = "dynamic";
    private static final String PROPERTIES = "properties";
    private static final String ID = "_id";
    private static final String PATH = "path";

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
     * dataset with no declared schema). Used by {@link Dataset#PARSER}. All declaration surfaces — column
     * {@code properties} and the meta-field {@code _id} — live inside {@code mappings}, so a dataset that only sets,
     * say, {@code _id.path} still needs a {@code mappings} wrapper.
     */
    @Nullable
    public static DatasetMapping assemble(@Nullable Mappings mappings) {
        return mappings == null ? null : new DatasetMapping(mappings);
    }

    /** Parses the {@code mappings} object ({@code dynamic}, {@code properties}, {@code _id}). */
    public static Mappings parseMappings(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        Dynamic dynamic = Dynamic.TRUE;
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        String idPath = null;
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
            } else if (ID.equals(field)) {
                // _id: { path: <column> } — the id-source column, a meta-field mirroring the index _id/alias path.
                // Only [path] is supported (identity from a column); any other key is rejected.
                ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
                XContentParser.Token t;
                while ((t = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (t == XContentParser.Token.FIELD_NAME) {
                        String key = parser.currentName();
                        if (PATH.equals(key) == false) {
                            throw new IllegalArgumentException("unknown [_id] field [" + key + "]; only [path] is supported");
                        }
                    } else {
                        idPath = parser.text();
                    }
                }
            } else {
                throw new IllegalArgumentException("unknown mappings field [" + field + "]");
            }
        }
        return new Mappings(dynamic, properties, idPath);
    }

    /** Emits the {@code mappings} block (incl. the {@code _id} meta-field) into an open dataset object. */
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
            if (mappings.idPath() != null) {
                builder.startObject(ID).field(PATH, mappings.idPath()).endObject();
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
