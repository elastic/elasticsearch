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

import static java.util.stream.Collectors.groupingBy;

public record Component(String name, HealthStatus status, List<HealthIndicator> indicators) implements ToXContentObject {

    public static List<Component> createFrom(List<HealthIndicator> indicators) {
        return indicators.stream()
            .collect(groupingBy(HealthIndicator::component))
            .entrySet()
            .stream()
            .map(
                entry -> new Component(
                    entry.getKey(),
                    HealthStatus.merge(entry.getValue().stream().map(HealthIndicator::status)),
                    entry.getValue()
                )
            )
            .toList();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("status", status);
        builder.startObject("indicators");
        for (HealthIndicator indicator : indicators) {
            builder.field(indicator.name());
            indicator.toXContent(builder, params);
        }
        builder.endObject();
        return builder.endObject();
    }
}
