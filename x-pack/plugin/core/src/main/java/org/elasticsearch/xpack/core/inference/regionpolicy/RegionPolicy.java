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
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public record RegionPolicy(@Nullable List<String> allowedGeos, @Nullable List<CspRegion> allowedRegions)
    implements
        ToXContentObject,
        Writeable {

    public static final ParseField ALLOWED_GEOS_FIELD = new ParseField("allowed_geos");
    public static final ParseField ALLOWED_REGIONS_FIELD = new ParseField("allowed_regions");

    public static final ConstructingObjectParser<RegionPolicy, Void> LENIENT_PARSER = createParser(true);
    public static final ConstructingObjectParser<RegionPolicy, Void> STRICT_PARSER = createParser(false);

    @SuppressWarnings("unchecked")
    public static ConstructingObjectParser<RegionPolicy, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<RegionPolicy, Void> parser = new ConstructingObjectParser<>(
            "region_policy",
            ignoreUnknownFields,
            args -> new RegionPolicy((List<String>) args[0], (List<CspRegion>) args[1])
        );
        parser.declareExclusiveFieldSet(ALLOWED_GEOS_FIELD.getPreferredName(), ALLOWED_REGIONS_FIELD.getPreferredName());
        parser.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), ALLOWED_GEOS_FIELD);
        parser.declareObjectArray(
            ConstructingObjectParser.optionalConstructorArg(),
            CspRegion.createParser(ignoreUnknownFields),
            ALLOWED_REGIONS_FIELD
        );
        return parser;
    }

    public RegionPolicy(StreamInput in) throws IOException {
        this(in.readOptionalStringCollectionAsList(), in.readOptionalCollectionAsList(CspRegion::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalStringCollection(allowedGeos);
        out.writeOptionalCollection(allowedRegions);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (allowedGeos != null) {
            builder.field(ALLOWED_GEOS_FIELD.getPreferredName(), allowedGeos);
        }
        if (allowedRegions != null) {
            builder.field(ALLOWED_REGIONS_FIELD.getPreferredName(), allowedRegions);
        }
        builder.endObject();
        return builder;
    }
}
