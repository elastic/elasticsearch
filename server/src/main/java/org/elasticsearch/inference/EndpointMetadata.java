/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public record EndpointMetadata(Heuristics heuristics, Internal internal, Display display) implements ToXContentObject, Writeable {

    public static final TransportVersion INFERENCE_ENDPOINT_METADATA_FIELDS_ADDED = TransportVersion.fromName(
        "inference_endpoint_metadata_fields_added"
    );

    public static final EndpointMetadata EMPTY = new EndpointMetadata(Heuristics.EMPTY, Internal.EMPTY, Display.EMPTY);

    public static final String METADATA = "metadata";
    public static final String HEURISTICS = "heuristics";
    public static final String INTERNAL = "internal";
    public static final String DISPLAY = "display";

    private static final String INCLUDE_INTERNAL_FIELDS = "include_internal_fields";

    private static final ConstructingObjectParser<EndpointMetadata, Void> PARSER = new ConstructingObjectParser<>(
        "endpoint_metadata_fields",
        true,
        args -> new EndpointMetadata(
            args[0] == null ? Heuristics.EMPTY : (Heuristics) args[0],
            args[1] == null ? Internal.EMPTY : (Internal) args[1],
            args[2] == null ? Display.EMPTY : (Display) args[2]
        )
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> Heuristics.parse(p), new ParseField(HEURISTICS));
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> Internal.parse(p), new ParseField(INTERNAL));
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> Display.parse(p), new ParseField(DISPLAY));
    }

    public static EndpointMetadata parse(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    public EndpointMetadata {
        Objects.requireNonNull(heuristics);
        Objects.requireNonNull(internal);
        Objects.requireNonNull(display);
    }

    public EndpointMetadata(StreamInput in) throws IOException {
        this(new Heuristics(in), new Internal(in), new Display(in));
    }

    public boolean isEmpty() {
        return this.equals(EMPTY);
    }

    public Params getXContentParamsExcludeInternalFields() {
        return new ToXContent.MapParams(Map.of(INCLUDE_INTERNAL_FIELDS, Boolean.FALSE.toString()));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(HEURISTICS, heuristics);

        if (params.paramAsBoolean(INCLUDE_INTERNAL_FIELDS, true)) {
            builder.field(INTERNAL, internal);
        }

        builder.field(DISPLAY, display);

        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return "EndpointMetadata{" + "heuristics=" + heuristics + ", internal=" + internal + ", display=" + display + '}';
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        heuristics.writeTo(out);
        internal.writeTo(out);
        display.writeTo(out);
    }

    public record Display(@Nullable String name) implements ToXContentObject, Writeable {

        public static final Display EMPTY = new Display((String) null);

        public static final String NAME = "name";

        private static final ConstructingObjectParser<Display, Void> PARSER = new ConstructingObjectParser<>(
            "endpoint_metadata_display",
            true,
            args -> new Display((String) args[0])
        );

        static {
            PARSER.declareStringOrNull(ConstructingObjectParser.optionalConstructorArg(), new ParseField(NAME));
        }

        public static Display parse(XContentParser parser) throws IOException {
            return PARSER.apply(parser, null);
        }

        public Display(StreamInput in) throws IOException {
            this(in.readOptionalString());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (name != null) {
                builder.field(NAME, name);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return "Display{" + "name=" + name + '}';
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(name);
        }
    }

    public record Heuristics(
        List<String> properties,
        @Nullable StatusHeuristic status,
        @Nullable LocalDate releaseDate,
        @Nullable LocalDate endOfLifeDate
    ) implements ToXContentObject, Writeable {

        public static final Heuristics EMPTY = new Heuristics(List.of(), null, (LocalDate) null, null);

        public static final String PROPERTIES = "properties";
        public static final String STATUS = "status";
        public static final String RELEASE_DATE = "release_date";
        public static final String END_OF_LIFE_DATE = "end_of_life_date";

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Heuristics, Void> PARSER = new ConstructingObjectParser<>(
            "endpoint_metadata_heuristics",
            true,
            args -> {
                List<String> properties = args[0] == null ? List.of() : (List<String>) args[0];
                var status = args[1] == null ? null : StatusHeuristic.fromString((String) args[1]);
                var releaseDate = args[2] == null ? null : LocalDate.parse((String) args[2]);
                var endOfLifeDate = args[3] == null ? null : LocalDate.parse((String) args[3]);

                return new Heuristics(properties, status, releaseDate, endOfLifeDate);
            }
        );

        static {
            PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), new ParseField(PROPERTIES));
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField(STATUS));
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField(RELEASE_DATE));
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField(END_OF_LIFE_DATE));
        }

        public static Heuristics parse(XContentParser parser) throws IOException {
            return PARSER.apply(parser, null);
        }

        public Heuristics(StreamInput in) throws IOException {
            this(
                in.readStringCollectionAsList(),
                in.readOptionalEnum(StatusHeuristic.class),
                in.readOptionalString(),
                in.readOptionalString()
            );
        }

        public Heuristics(
            List<String> properties,
            @Nullable StatusHeuristic status,
            @Nullable String releaseDate,
            @Nullable String endOfLifeDate
        ) {
            this(
                properties,
                status,
                releaseDate != null ? LocalDate.parse(releaseDate) : null,
                endOfLifeDate != null ? LocalDate.parse(endOfLifeDate) : null
            );
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            builder.field(PROPERTIES, properties);

            if (status != null) {
                builder.field(STATUS, status);
            }
            if (releaseDate != null) {
                builder.field(RELEASE_DATE, releaseDate.toString());
            }
            if (endOfLifeDate != null) {
                builder.field(END_OF_LIFE_DATE, endOfLifeDate.toString());
            }

            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return "Heuristics{"
                + "properties="
                + properties
                + ", status='"
                + status
                + '\''
                + ", releaseDate='"
                + releaseDate
                + '\''
                + ", endOfLifeDate='"
                + endOfLifeDate
                + '\''
                + '}';
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(properties);
            out.writeOptionalEnum(status);
            out.writeOptionalString(releaseDate != null ? releaseDate.toString() : null);
            out.writeOptionalString(endOfLifeDate != null ? endOfLifeDate.toString() : null);
        }
    }

    public record Internal(@Nullable String fingerprint, @Nullable Long version) implements ToXContentObject, Writeable {

        public static final Internal EMPTY = new Internal(null, null);

        public static final String FINGERPRINT = "fingerprint";
        public static final String VERSION = "version";

        private static final ConstructingObjectParser<Internal, Void> PARSER = new ConstructingObjectParser<>(
            "endpoint_metadata_internal",
            true,
            args -> new Internal((String) args[0], (Long) args[1])
        );

        static {
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField(FINGERPRINT));
            PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), new ParseField(VERSION));
        }

        public static Internal parse(XContentParser parser) throws IOException {
            return PARSER.apply(parser, null);
        }

        public Internal(StreamInput in) throws IOException {
            this(in.readOptionalString(), in.readOptionalVLong());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            if (fingerprint != null) {
                builder.field(FINGERPRINT, fingerprint);
            }

            if (version != null) {
                builder.field(VERSION, version);
            }

            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return "Internal{" + "fingerprint=" + fingerprint + ", version=" + version + '}';
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(fingerprint);
            out.writeOptionalVLong(version);
        }
    }
}
