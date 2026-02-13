/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.metadata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.StatusHeuristic;
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

/**
 * Endpoint metadata contains descriptive information for an inference endpoint. This information allows an upstream service to communicate
 * the features a particular model supports and other properties to help clients of the Inference API determine which model to use for
 * defaults in different scenarios.
 * <p>
 * The Elastic Inference Service populates these fields so that Kibana and semantic text fields determine the correct defaults.
 *
 * @param heuristics contains information so clients of the Inference API can determine which models should be used as defaults and
 *                   presented to users in different scenarios.
 * @param internal   contains information that is only used within Elasticsearch. The internal information helps the Inference API know
 *                   when it needs to update the preconfigured endpoints by tracking the upstream fingerprint and an internal version.
 * @param display    contains information for how to display the endpoint in user interfaces (descriptive name, etc).
 */
public record EndpointMetadata(Heuristics heuristics, Internal internal, Display display) implements ToXContentObject, Writeable {

    public static final TransportVersion INFERENCE_ENDPOINT_METADATA_FIELDS_ADDED = TransportVersion.fromName(
        "inference_endpoint_metadata_fields_added"
    );
    public static final EndpointMetadata EMPTY_INSTANCE = new EndpointMetadata(
        Heuristics.EMPTY_INSTANCE,
        Internal.EMPTY_INSTANCE,
        Display.EMPTY_INSTANCE
    );
    public static final String METADATA_FIELD_NAME = "metadata";
    public static final String HEURISTICS_FIELD_NAME = "heuristics";
    public static final String INTERNAL_FIELD_NAME = "internal";
    public static final String DISPLAY_FIELD_NAME = "display";

    private static final String INCLUDE_INTERNAL_FIELDS_PARAM_NAME = "include_internal_fields";

    private static final ConstructingObjectParser<EndpointMetadata, Void> PARSER = new ConstructingObjectParser<>(
        "endpoint_metadata_fields",
        true,
        args -> new EndpointMetadata(
            args[0] == null ? Heuristics.EMPTY_INSTANCE : (Heuristics) args[0],
            args[1] == null ? Internal.EMPTY_INSTANCE : (Internal) args[1],
            args[2] == null ? Display.EMPTY_INSTANCE : (Display) args[2]
        )
    );

    static {
        PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> Heuristics.parse(p),
            new ParseField(HEURISTICS_FIELD_NAME)
        );
        PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> Internal.parse(p),
            new ParseField(INTERNAL_FIELD_NAME)
        );
        PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> Display.parse(p),
            new ParseField(DISPLAY_FIELD_NAME)
        );
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
        return this.equals(EMPTY_INSTANCE);
    }

    public Params getXContentParamsExcludeInternalFields() {
        return new ToXContent.MapParams(Map.of(INCLUDE_INTERNAL_FIELDS_PARAM_NAME, Boolean.FALSE.toString()));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(HEURISTICS_FIELD_NAME, heuristics);

        if (params.paramAsBoolean(INCLUDE_INTERNAL_FIELDS_PARAM_NAME, true)) {
            builder.field(INTERNAL_FIELD_NAME, internal);
        }

        builder.field(DISPLAY_FIELD_NAME, display);

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

        public static final Display EMPTY_INSTANCE = new Display((String) null);
        public static final String NAME_FIELD = "name";
        private static final ConstructingObjectParser<Display, Void> PARSER = new ConstructingObjectParser<>(
            "endpoint_metadata_display",
            true,
            args -> new Display((String) args[0])
        );

        static {
            PARSER.declareStringOrNull(ConstructingObjectParser.optionalConstructorArg(), new ParseField(NAME_FIELD));
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
                builder.field(NAME_FIELD, name);
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

        public boolean isEmpty() {
            return this.equals(EMPTY_INSTANCE);
        }
    }

    /**
     * Heuristics about the model used in the endpoint to help clients of the Inference API determine which models to use.
     *
     * @param properties    a list of string tags describing the model's properties (e.g., "multilingual", "english", "multimodal", etc)
     * @param status        the stability of the model used in the endpoint
     * @param releaseDate   the release date of the model used in the endpoint
     * @param endOfLifeDate the end-of-life date of the model used in the endpoint
     */
    public record Heuristics(
        List<String> properties,
        @Nullable StatusHeuristic status,
        @Nullable LocalDate releaseDate,
        @Nullable LocalDate endOfLifeDate
    ) implements ToXContentObject, Writeable {

        public static final Heuristics EMPTY_INSTANCE = new Heuristics(List.of(), null, (LocalDate) null, null);

        public static final String PROPERTIES_FIELD_NAME = "properties";
        public static final String STATUS_FIELD_NAME = "status";
        public static final String RELEASE_DATE_FIELD_NAME = "release_date";
        public static final String END_OF_LIFE_DATE_FIELD_NAME = "end_of_life_date";

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
            PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), new ParseField(PROPERTIES_FIELD_NAME));
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField(STATUS_FIELD_NAME));
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField(RELEASE_DATE_FIELD_NAME));
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField(END_OF_LIFE_DATE_FIELD_NAME));
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

            builder.field(PROPERTIES_FIELD_NAME, properties);

            if (status != null) {
                builder.field(STATUS_FIELD_NAME, status);
            }
            if (releaseDate != null) {
                builder.field(RELEASE_DATE_FIELD_NAME, releaseDate.toString());
            }
            if (endOfLifeDate != null) {
                builder.field(END_OF_LIFE_DATE_FIELD_NAME, endOfLifeDate.toString());
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

        public boolean isEmpty() {
            return this.equals(EMPTY_INSTANCE);
        }
    }

    /**
     * Internal metadata used by Elasticsearch. This information is not exposed to clients of the Inference API. It helps the Inference
     * API know when it needs to update the preconfigured endpoints.
     *
     * @param fingerprint an upstream fingerprint representing endpoint version. The upstream service controls this to indicate that known
     *                    fields have changed (new properties for a model, status, etc).
     * @param version     a schema version number for the metadata. This version is incremented when the structure of the metadata changes
     *                    (e.g., new fields are added). This allows Elasticsearch to determine when it needs to update the metadata even if
     *                    the upstream fingerprint is not changed. This is useful when the fingerprint has already changed but older
     *                    Elasticsearch versions don't have the logic to handle a new field returned by the upstream service. This value
     *                    allows Elasticsearch to determine which endpoints need to have their schema updated in a new version.
     */
    public record Internal(@Nullable String fingerprint, @Nullable Long version) implements ToXContentObject, Writeable {

        public static final Internal EMPTY_INSTANCE = new Internal(null, null);

        public static final String FINGERPRINT_FIELD_NAME = "fingerprint";
        public static final String VERSION_FIELD_NAME = "version";

        private static final ConstructingObjectParser<Internal, Void> PARSER = new ConstructingObjectParser<>(
            "endpoint_metadata_internal",
            true,
            args -> new Internal((String) args[0], (Long) args[1])
        );

        static {
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField(FINGERPRINT_FIELD_NAME));
            PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), new ParseField(VERSION_FIELD_NAME));
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
                builder.field(FINGERPRINT_FIELD_NAME, fingerprint);
            }

            if (version != null) {
                builder.field(VERSION_FIELD_NAME, version);
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

        public boolean isEmpty() {
            return this.equals(EMPTY_INSTANCE);
        }
    }
}
