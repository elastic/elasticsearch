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

    public static final HealthIndicatorImpact EMPTY = new HealthIndicatorImpact();
    private static final int EMPTY_OBJECT_SEVERITY = -99;

    private HealthIndicatorImpact() {
        this(EMPTY_OBJECT_SEVERITY, null);
    }

    public HealthIndicatorImpact {
        if ((severity == EMPTY_OBJECT_SEVERITY && impactDescription == null) == false) {
            if (severity < 0) {
                throw new IllegalArgumentException("Severity cannot be less than 0");
            }
            Objects.requireNonNull(impactDescription);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (severity > 0) {
            builder.field("severity", severity);
            builder.field("impact_description", impactDescription);
        }
        builder.endObject();
        return builder;
    }
}
