/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public record HealthIndicatorImpact(int severity, String impactDescription) implements ToXContentObject {

    public HealthIndicatorImpact {
        if (severity < 0) {
            throw new IllegalArgumentException("Severity cannot be less than 0");
        }
        Objects.requireNonNull(impactDescription);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("severity", severity);
        builder.field("description", impactDescription);
        builder.endObject();
        return builder;
    }
}
