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
 * A user-declared schema attached to a {@link Dataset}. Entirely optional — a dataset with no
 * {@code DatasetSchema} resolves its schema by inference, exactly as before.
 *
 * <p>Groups the three declaration surfaces of the dataset PUT body:
 * <ul>
 *   <li>the {@code mappings} block ({@link Mappings}: a {@code dynamic} mode + per-column {@code properties}),
 *       which may be absent (role-only declarations);</li>
 *   <li>{@code timestamp_field} — names the column that is the time axis;</li>
 *   <li>{@code id_field} — names the column that backs {@code _id}.</li>
 * </ul>
 *
 * <p>The role designations are <b>orthogonal to the mappings</b>: any of them may be present without the
 * others, and they point at a column in the <i>resolved</i> schema (declared or inferred), so they work
 * under full inference too. They are stored as plain Strings; validation that they reference a real column
 * (and that a {@code timestamp_field} resolves to a date type) happens in the ES|QL layer — at put time when
 * the column is declared, otherwise at first query.
 *
 * <p>Like {@link DataSourceReference}, this has no standalone XContent: {@link Dataset#toXContent} emits the
 * flat {@code mappings}/{@code timestamp_field}/{@code id_field} keys and {@link Dataset#PARSER} reads them
 * back, assembling this object via {@link #assemble}. That keeps a single on-disk JSON shape.
 */
public final class DatasetSchema implements Writeable {

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
     * {@code properties} is order-preserving and may be empty (e.g. {@code "mappings": { "dynamic": "false" }}).
     */
    public record Mappings(Dynamic dynamic, Map<String, DatasetFieldMapping> properties) implements Writeable {

        public Mappings {
            Objects.requireNonNull(dynamic, "dynamic must not be null");
            properties = properties == null ? Map.of() : Collections.unmodifiableMap(properties);
        }

        Mappings(StreamInput in) throws IOException {
            this(Dynamic.values()[in.readVInt()], in.readOrderedMap(StreamInput::readString, DatasetFieldMapping::new));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(dynamic.ordinal());
            out.writeMap(properties, (o, v) -> v.writeTo(o));
        }
    }

    private static final String DYNAMIC = "dynamic";
    private static final String PROPERTIES = "properties";

    @Nullable
    private final Mappings mappings;
    @Nullable
    private final String timestampField;
    @Nullable
    private final String idField;

    public DatasetSchema(@Nullable Mappings mappings, @Nullable String timestampField, @Nullable String idField) {
        this.mappings = mappings;
        this.timestampField = timestampField;
        this.idField = idField;
    }

    public DatasetSchema(StreamInput in) throws IOException {
        this.mappings = in.readOptionalWriteable(Mappings::new);
        this.timestampField = in.readOptionalString();
        this.idField = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(mappings);
        out.writeOptionalString(timestampField);
        out.writeOptionalString(idField);
    }

    /**
     * Builds a {@link DatasetSchema} from the three parsed top-level pieces, or {@code null} when none are present
     * (a dataset with no declared schema). Used by {@link Dataset#PARSER}.
     */
    @Nullable
    public static DatasetSchema assemble(@Nullable Mappings mappings, @Nullable String timestampField, @Nullable String idField) {
        if (mappings == null && timestampField == null && idField == null) {
            return null;
        }
        return new DatasetSchema(mappings, timestampField, idField);
    }

    /** Parses the {@code mappings} object ({@code dynamic} + {@code properties}) into a {@link Mappings}. */
    public static Mappings parseMappings(XContentParser parser) throws IOException {
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
            } else {
                throw new IllegalArgumentException("unknown mappings field [" + field + "]");
            }
        }
        return new Mappings(dynamic, properties);
    }

    /** Emits the flat {@code mappings}/{@code timestamp_field}/{@code id_field} keys into an already-open dataset object. */
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
        if (timestampField != null) {
            builder.field("timestamp_field", timestampField);
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
    public String timestampField() {
        return timestampField;
    }

    @Nullable
    public String idField() {
        return idField;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DatasetSchema that = (DatasetSchema) o;
        return Objects.equals(mappings, that.mappings)
            && Objects.equals(timestampField, that.timestampField)
            && Objects.equals(idField, that.idField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mappings, timestampField, idField);
    }

    @Override
    public String toString() {
        return "DatasetSchema[mappings=" + mappings + ", timestampField=" + timestampField + ", idField=" + idField + "]";
    }
}
