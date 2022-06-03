/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public record HealthIndicatorImpact(int severity, String impactDescription, List<ImpactArea> impactAreas)
    implements
        ToXContentObject,
        Writeable {

    public HealthIndicatorImpact(StreamInput in) throws IOException {
        this(in.readInt(), in.readString(), in.readStringList().stream().map(ImpactArea::valueOf).toList());
    }

    public HealthIndicatorImpact {
        if (severity < 0) {
            throw new IllegalArgumentException("Severity cannot be less than 0");
        }
        if (Strings.isEmpty(impactDescription)) {
            throw new IllegalArgumentException("Impact description must be provided");
        }
        if (impactAreas == null || impactAreas.isEmpty()) {
            throw new IllegalArgumentException("At least one impact area must be provided");
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("severity", severity);
        builder.field("description", impactDescription);
        builder.startArray("impact_areas");
        for (ImpactArea impactArea : impactAreas) {
            builder.value(impactArea.displayValue());
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(severity);
        out.writeString(impactDescription);
        out.writeStringCollection(impactAreas.stream().map(Enum::name).toList());
    }
}
