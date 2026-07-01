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
 * <p>Groups the declaration surfaces of the dataset PUT body:
 * <ul>
 *   <li>the {@code mappings} block ({@link Mappings}: a {@code dynamic} mode + per-column {@code properties}),
 *       which may be absent (a role-only declaration);</li>
 *   <li>{@code id_field} — names the column whose value is copied into {@code _id}.</li>
 * </ul>
 *
 * <p>There is <b>no timestamp role</b>: a time axis is just a column named {@code @timestamp}, declared as an
 * ordinary rename (e.g. {@code "@timestamp": {"type":"date","source":"ts"}}) and recognized by the stack by
 * name — a rename ("move"), not a designation. {@code id_field} stays a role because {@code _id} is a metadata
 * slot, not a nameable column: it <i>copies</i> a column's value (the column remains). {@code id_field} is
 * orthogonal to the mappings — it may be present without them, points at a column in the <i>resolved</i> schema
 * (declared or inferred), and is stored as a plain String; validation that it references a real column happens
 * in the ES|QL layer — at put time when the column is declared, otherwise at first query.
 *
 * <p>Like {@link DataSourceReference}, this has no standalone XContent: {@link Dataset#toXContent} emits the
 * flat {@code mappings}/{@code id_field} keys and {@link Dataset#PARSER} reads them back, assembling this
 * object via {@link #assemble}. That keeps a single on-disk JSON shape.
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
     * @param dynamic       undeclared-column policy ({@code true} = infer + overlay, {@code false} = declaration is the
     *                      whole schema).
     * @param properties    per-column declarations keyed by logical name; order-preserving, may be empty (e.g.
     *                      {@code "mappings": { "dynamic": "false" }}).
     * @param sourceEnabled {@code _source.enabled}: whether a synthetic {@code _source} is produced for the dataset's
     *                      rows. {@code null} means unset — the default ({@code true}, source available). Mirrors the
     *                      core {@code _source} mapping's {@code enabled}, restricted to the read-applicable knob.
     */
    public record Mappings(Dynamic dynamic, Map<String, DatasetFieldMapping> properties, @Nullable Boolean sourceEnabled)
        implements
            Writeable {

        public Mappings {
            Objects.requireNonNull(dynamic, "dynamic must not be null");
            properties = properties == null ? Map.of() : Collections.unmodifiableMap(properties);
        }

        /** Back-compat convenience: a mappings block with no {@code _source} knob (source enabled by default). */
        public Mappings(Dynamic dynamic, Map<String, DatasetFieldMapping> properties) {
            this(dynamic, properties, null);
        }

        Mappings(StreamInput in) throws IOException {
            // The whole DatasetMapping is gated by the dataset_declared_schema transport version (see Dataset), which is
            // unreleased — so every field, including _source.enabled, ships in that one version; no separate gate needed.
            this(
                in.readEnum(Dynamic.class),
                in.readOrderedMap(StreamInput::readString, DatasetFieldMapping::new),
                in.readOptionalBoolean()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(dynamic);
            out.writeMap(properties, (o, v) -> v.writeTo(o));
            out.writeOptionalBoolean(sourceEnabled);
        }

        /** {@code _source.enabled} resolved to its effective value: {@code true} (available) unless explicitly disabled. */
        public boolean sourceAvailable() {
            return sourceEnabled == null || sourceEnabled;
        }
    }

    private static final String DYNAMIC = "dynamic";
    private static final String PROPERTIES = "properties";
    private static final String SOURCE = "_source";
    private static final String ENABLED = "enabled";

    @Nullable
    private final Mappings mappings;
    @Nullable
    private final String idField;

    public DatasetMapping(@Nullable Mappings mappings, @Nullable String idField) {
        this.mappings = mappings;
        this.idField = idField;
    }

    public DatasetMapping(StreamInput in) throws IOException {
        this.mappings = in.readOptionalWriteable(Mappings::new);
        this.idField = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(mappings);
        out.writeOptionalString(idField);
    }

    /**
     * Builds a {@link DatasetMapping} from the parsed top-level pieces, or {@code null} when none are present (a
     * dataset with no declared schema). Used by {@link Dataset#PARSER}. The timestamp is <b>not</b> a role here: a
     * time axis is declared as an ordinary rename to {@code @timestamp} (e.g.
     * {@code "@timestamp": {"type":"date","source":"ts"}}), which the stack recognizes by name — no designation.
     */
    @Nullable
    public static DatasetMapping assemble(@Nullable Mappings mappings, @Nullable String idField) {
        if (mappings == null && idField == null) {
            return null;
        }
        return new DatasetMapping(mappings, idField);
    }

    /** Parses the {@code mappings} object ({@code dynamic} + {@code properties}) into a {@link Mappings}. */
    public static Mappings parseMappings(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        Dynamic dynamic = Dynamic.TRUE;
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        Boolean sourceEnabled = null;
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
            } else if (SOURCE.equals(field)) {
                // _source: { enabled: <bool> } — the only supported knob (mirrors the core _source mapping, read-side).
                ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
                XContentParser.Token t;
                while ((t = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (t == XContentParser.Token.FIELD_NAME) {
                        String key = parser.currentName();
                        if (ENABLED.equals(key) == false) {
                            throw new IllegalArgumentException("unknown [_source] field [" + key + "]; only [enabled] is supported");
                        }
                    } else {
                        sourceEnabled = parser.booleanValue();
                    }
                }
            } else {
                throw new IllegalArgumentException("unknown mappings field [" + field + "]");
            }
        }
        return new Mappings(dynamic, properties, sourceEnabled);
    }

    /** Emits the flat {@code mappings}/{@code id_field} keys into an already-open dataset object. */
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
            if (mappings.sourceEnabled() != null) {
                builder.startObject(SOURCE).field(ENABLED, mappings.sourceEnabled()).endObject();
            }
            builder.endObject();
        }
        if (idField != null) {
            builder.field("id_field", idField);
        }
    }

    @Nullable
    public Mappings mappings() {
        return mappings;
    }

    @Nullable
    public String idField() {
        return idField;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DatasetMapping that = (DatasetMapping) o;
        return Objects.equals(mappings, that.mappings) && Objects.equals(idField, that.idField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mappings, idField);
    }

    @Override
    public String toString() {
        return "DatasetMapping[mappings=" + mappings + ", idField=" + idField + "]";
    }
}
