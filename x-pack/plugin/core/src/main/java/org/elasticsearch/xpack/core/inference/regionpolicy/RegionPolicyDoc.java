/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.regionpolicy;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceIndexDocTypeField;
import org.elasticsearch.inference.ToXContentParams;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.common.time.TimeUtils;

import java.io.IOException;
import java.time.Instant;

public record RegionPolicyDoc(
    RegionPolicy regionPolicy,
    Instant createdAt,
    @Nullable String createdBy,
    @Nullable Instant updatedAt,
    @Nullable String updatedBy
) implements ToXContentObject, Writeable {

    public static final ParseField REGION_POLICY_FIELD = new ParseField("region_policy");
    public static final ParseField CREATED_AT_FIELD = new ParseField("created_at");
    public static final ParseField CREATED_BY_FIELD = new ParseField("created_by");
    public static final ParseField UPDATED_AT_FIELD = new ParseField("updated_at");
    public static final ParseField UPDATED_BY_FIELD = new ParseField("updated_by");

    /**
     * The document id for region policies.
     * We currently only support region policy for the elastic inference service.
     * We add "elastic" to the document id to avoid conflicts with other inference services in the future.
     */
    public static final String DOCUMENT_ID = "region_policy_elastic";

    public static final ConstructingObjectParser<RegionPolicyDoc, Void> STRICT_PARSER = createParser(false);
    public static final ConstructingObjectParser<RegionPolicyDoc, Void> LENIENT_PARSER = createParser(true);

    static ConstructingObjectParser<RegionPolicyDoc, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<RegionPolicyDoc, Void> parser = new ConstructingObjectParser<>(
            "region_policy_doc",
            ignoreUnknownFields,
            args -> new RegionPolicyDoc((RegionPolicy) args[0], (Instant) args[1], (String) args[2], (Instant) args[3], (String) args[4])
        );
        parser.declareObject(
            ConstructingObjectParser.constructorArg(),
            RegionPolicy.createParser(ignoreUnknownFields),
            REGION_POLICY_FIELD
        );
        parser.declareField(
            ConstructingObjectParser.constructorArg(),
            p -> TimeUtils.parseTimeFieldToInstant(p, CREATED_AT_FIELD.getPreferredName()),
            CREATED_AT_FIELD,
            ObjectParser.ValueType.VALUE
        );
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), CREATED_BY_FIELD);
        parser.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            p -> TimeUtils.parseTimeFieldToInstant(p, UPDATED_AT_FIELD.getPreferredName()),
            UPDATED_AT_FIELD,
            ObjectParser.ValueType.VALUE
        );
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), UPDATED_BY_FIELD);
        return parser;
    }

    public RegionPolicyDoc(StreamInput in) throws IOException {
        this(new RegionPolicy(in), in.readInstant(), in.readOptionalString(), in.readOptionalInstant(), in.readOptionalString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        regionPolicy.writeTo(out);
        out.writeInstant(createdAt);
        out.writeOptionalString(createdBy);
        out.writeOptionalInstant(updatedAt);
        out.writeOptionalString(updatedBy);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (params.paramAsBoolean(ToXContentParams.FOR_INTERNAL_STORAGE, false)) {
            builder.field(InferenceIndexDocTypeField.DOC_TYPE_FIELD, InferenceIndexDocTypeField.REGION_POLICY_TYPE);
        }
        builder.field(REGION_POLICY_FIELD.getPreferredName(), regionPolicy);
        builder.field(CREATED_AT_FIELD.getPreferredName(), createdAt);
        if (createdBy != null) {
            builder.field(CREATED_BY_FIELD.getPreferredName(), createdBy);
        }
        if (updatedAt != null) {
            builder.field(UPDATED_AT_FIELD.getPreferredName(), updatedAt);
        }
        if (updatedBy != null) {
            builder.field(UPDATED_BY_FIELD.getPreferredName(), updatedBy);
        }
        builder.endObject();
        return builder;
    }
}
