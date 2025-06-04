/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.TransportVersions.RESCORE_VECTOR_ALLOW_ZERO;
import static org.elasticsearch.TransportVersions.RESCORE_VECTOR_ALLOW_ZERO_BACKPORT_8_19;

public class RescoreVectorBuilder implements Writeable, ToXContentObject {

    public static final ParseField OVERSAMPLE_FIELD = new ParseField("oversample");
    public static final float NO_OVERSAMPLE = 0.0F;
    public static final float MIN_OVERSAMPLE = 1.0F;
    private static final ConstructingObjectParser<RescoreVectorBuilder, Void> PARSER = new ConstructingObjectParser<>(
        "rescore_vector",
        args -> new RescoreVectorBuilder((Float) args[0])
    );

    static {
        PARSER.declareFloat(ConstructingObjectParser.constructorArg(), OVERSAMPLE_FIELD);
    }

    // Oversample is required as of now as it is the only field in the rescore vector
    private final float oversample;

    public RescoreVectorBuilder(float numCandidatesFactor) {
        Objects.requireNonNull(numCandidatesFactor, "[" + OVERSAMPLE_FIELD.getPreferredName() + "] must be set");
        if (numCandidatesFactor < MIN_OVERSAMPLE && numCandidatesFactor != NO_OVERSAMPLE) {
            throw new IllegalArgumentException("[" + OVERSAMPLE_FIELD.getPreferredName() + "] must be >= " + MIN_OVERSAMPLE + " or 0");
        }
        this.oversample = numCandidatesFactor;
    }

    public RescoreVectorBuilder(StreamInput in) throws IOException {
        this.oversample = in.readFloat();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // We don't want to serialize a `0` oversample to a node that doesn't know what to do with it.
        if (oversample == NO_OVERSAMPLE
            && out.getTransportVersion().before(RESCORE_VECTOR_ALLOW_ZERO)
            && out.getTransportVersion().isPatchFrom(RESCORE_VECTOR_ALLOW_ZERO_BACKPORT_8_19) == false) {
            throw new ElasticsearchStatusException(
                "[rescore_vector] does not support a 0 for ["
                    + OVERSAMPLE_FIELD.getPreferredName()
                    + "] before version ["
                    + RESCORE_VECTOR_ALLOW_ZERO.toReleaseVersion()
                    + "]",
                RestStatus.BAD_REQUEST
            );
        }
        out.writeFloat(oversample);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(OVERSAMPLE_FIELD.getPreferredName(), oversample);
        builder.endObject();
        return builder;
    }

    public static RescoreVectorBuilder fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RescoreVectorBuilder that = (RescoreVectorBuilder) o;
        return Objects.equals(oversample, that.oversample);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(oversample);
    }

    public float oversample() {
        return oversample;
    }
}
