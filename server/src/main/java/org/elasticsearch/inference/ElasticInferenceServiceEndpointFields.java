/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public record ElasticInferenceServiceEndpointFields(Metadata metadata, Internal internal) implements ToXContentFragment, Writeable {

    public static final ElasticInferenceServiceEndpointFields EMPTY = new ElasticInferenceServiceEndpointFields(
        Metadata.EMPTY,
        Internal.EMPTY
    );

    static final String DESCRIPTION = "description";

    private static final String INCLUDE_INTERNAL_FIELDS = "include_internal_fields";
    private static final String METADATA = "metadata";
    private static final String INTERNAL = "internal";

    private static final ConstructingObjectParser<ElasticInferenceServiceEndpointFields, Void> PARSER = new ConstructingObjectParser<>(
        "elastic_inference_service_endpoint_fields",
        true,
        args -> new ElasticInferenceServiceEndpointFields(
            args[0] == null ? Metadata.EMPTY : (Metadata) args[0],
            args[1] == null ? Internal.EMPTY : (Internal) args[1]
        )
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> Metadata.parse(p), new ParseField(METADATA));
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> Internal.parse(p), new ParseField(INTERNAL));
    }

    public static ElasticInferenceServiceEndpointFields parse(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    public ElasticInferenceServiceEndpointFields(StreamInput in) throws IOException {
        this(new Metadata(in), new Internal(in));
    }

    public XContentBuilder toXContentWithoutInternalFields(XContentBuilder builder) throws IOException {
        return toXContent(builder, new ToXContent.MapParams(Map.of(INCLUDE_INTERNAL_FIELDS, Boolean.FALSE.toString())));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(DESCRIPTION);

        builder.field(METADATA, metadata);

        if (params.paramAsBoolean(INCLUDE_INTERNAL_FIELDS, true)) {
            builder.field(INTERNAL, internal);
        }

        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return "ElasticInferenceServiceEndpointFields{" + "metadata=" + metadata + ", internal=" + internal + '}';
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        metadata.writeTo(out);
        internal.writeTo(out);
    }

    public record Metadata(List<String> properties, @Nullable String status, @Nullable String releaseDate, @Nullable String endOfLifeDate)
        implements
            ToXContentObject,
            Writeable {

        public static final Metadata EMPTY = new Metadata(List.of(), null, null, null);

        public static final String PROPERTIES = "properties";
        public static final String STATUS = "status";
        public static final String RELEASE_DATE = "release_date";
        public static final String END_OF_LIFE_DATE = "end_of_life_date";

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Metadata, Void> PARSER = new ConstructingObjectParser<>(
            "elastic_inference_service_endpoint_metadata",
            true,
            args -> new Metadata(args[0] == null ? List.of() : (List<String>) args[0], (String) args[1], (String) args[2], (String) args[3])
        );

        static {
            PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), new ParseField(PROPERTIES));
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField(STATUS));
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField(RELEASE_DATE));
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField(END_OF_LIFE_DATE));
        }

        public static Metadata parse(XContentParser parser) throws IOException {
            return PARSER.apply(parser, null);
        }

        public Metadata(StreamInput in) throws IOException {
            this(in.readStringCollectionAsList(), in.readOptionalString(), in.readOptionalString(), in.readOptionalString());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            builder.field(PROPERTIES, properties);

            if (status != null) {
                builder.field(STATUS, status);
            }
            if (releaseDate != null) {
                builder.field(RELEASE_DATE, releaseDate);
            }
            if (endOfLifeDate != null) {
                builder.field(END_OF_LIFE_DATE, endOfLifeDate);
            }

            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return "Metadata{"
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
            out.writeOptionalString(status);
            out.writeOptionalString(releaseDate);
            out.writeOptionalString(endOfLifeDate);
        }
    }

    public record Internal(@Nullable String metadataFieldsHash, @Nullable String version) implements ToXContentObject, Writeable {

        public static final Internal EMPTY = new Internal(null, null);

        public static final String METADATA_FIELDS_HASH = "metadata_fields_hash";
        public static final String VERSION = "version";

        private static final ConstructingObjectParser<Internal, Void> PARSER = new ConstructingObjectParser<>(
            "elastic_inference_service_endpoint_internal",
            true,
            args -> new Internal((String) args[0], (String) args[1])
        );

        static {
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField(METADATA_FIELDS_HASH));
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField(VERSION));
        }

        public static Internal parse(XContentParser parser) throws IOException {
            return PARSER.apply(parser, null);
        }

        public Internal(StreamInput in) throws IOException {
            this(in.readOptionalString(), in.readOptionalString());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            if (metadataFieldsHash != null) {
                builder.field(METADATA_FIELDS_HASH, metadataFieldsHash);
            }

            if (version != null) {
                builder.field(VERSION, version);
            }

            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return "Internal{" + "metadataFieldsHash='" + metadataFieldsHash + '\'' + ", version='" + version + '\'' + '}';
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(metadataFieldsHash);
            out.writeOptionalString(version);
        }
    }
}
