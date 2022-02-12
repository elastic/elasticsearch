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
import java.util.NoSuchElementException;
import java.util.Objects;

public record HealthComponentResult(String name, HealthStatus status, List<HealthIndicatorResult> indicators) implements ToXContentObject {

    public HealthIndicatorResult findIndicator(String name) {
        return indicators.stream()
            .filter(i -> Objects.equals(i.name(), name))
            .findFirst()
            .orElseThrow(() -> new NoSuchElementException("Indicator [" + name + "] is not found"));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("status", status.xContentValue());
        builder.startObject("indicators");
        for (HealthIndicatorResult indicator : indicators) {
            builder.field(indicator.name(), indicator, params);
        }
        builder.endObject();
        return builder.endObject();
    }
}
