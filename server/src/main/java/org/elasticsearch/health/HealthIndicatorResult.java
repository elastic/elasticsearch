/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public record HealthIndicatorResult(
    String name,
    String component,
    HealthStatus status,
    String summary,
    HealthIndicatorDetails details,
    List<HealthIndicatorImpact> impacts,
    @Nullable List<UserAction> userActions
) implements ToXContentObject {

    public HealthIndicatorResult(String name, String component, HealthStatus status, String summary, HealthIndicatorDetails details) {
        this(name, component, status, summary, details, Collections.emptyList(), null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("status", status.xContentValue());
        builder.field("summary", summary);
        builder.field("details", details, params);
        if (impacts != null && impacts.isEmpty() == false) {
            builder.field("impacts", impacts);
        }
        // TODO 83303: Add detail / documentation
        if (userActions != null && userActions.size() > 0) {
            builder.field("user_actions", userActions);
        }
        return builder.endObject();
    }
}
