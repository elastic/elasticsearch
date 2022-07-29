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
import java.util.List;

public record HealthIndicatorResult(
    String name,
    HealthStatus status,
    String symptom,
    HealthIndicatorDetails details,
    List<HealthIndicatorImpact> impacts,
    List<Diagnosis> diagnosisList
) implements ToXContentObject {

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("status", status.xContentValue());
        builder.field("symptom", symptom);
        if (details != null && HealthIndicatorDetails.EMPTY.equals(details) == false) {
            builder.field("details", details, params);
        }
        if (impacts != null && impacts.isEmpty() == false) {
            builder.field("impacts", impacts);
        }
        if (diagnosisList != null && diagnosisList.isEmpty() == false) {
            builder.field("diagnosis", diagnosisList);
        }
        return builder.endObject();
    }
}
