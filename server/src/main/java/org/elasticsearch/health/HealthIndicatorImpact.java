/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

public record HealthIndicatorImpact(String id, int severity, String impactDescription, List<ImpactArea> impactAreas)
    implements
        ToXContentObject {

    private static final Pattern ID_PATTERN = Pattern.compile("elasticsearch:health:[a-z_]+:impact:[a-z_:]+");

    public HealthIndicatorImpact {
        if (ID_PATTERN.matcher(id).matches() == false) {
            throw new IllegalArgumentException("Invalid hierarchical id prefix");
        }
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
        builder.field("id", id);
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
}
