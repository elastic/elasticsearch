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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

public record HealthComponentResult(String name, HealthStatus status, Map<String, HealthIndicatorResult> indicators)
    implements
        ToXContentObject {

    public static Collection<HealthComponentResult> createComponentsFromIndicators(Collection<HealthIndicatorResult> indicators) {
        return indicators.stream()
            .collect(
                groupingBy(
                    HealthIndicatorResult::component,
                    TreeMap::new,
                    collectingAndThen(toList(), HealthComponentResult::createComponentFromIndicators)
                )
            )
            .values();
    }

    private static HealthComponentResult createComponentFromIndicators(List<HealthIndicatorResult> indicators) {
        assert indicators.size() > 0 : "Component should not be non empty";
        assert indicators.stream().map(HealthIndicatorResult::component).distinct().count() == 1L
            : "Should not mix indicators from different components";
        return new HealthComponentResult(
            indicators.get(0).component(),
            HealthStatus.merge(indicators.stream().map(HealthIndicatorResult::status)),
            indicators.stream().collect(Collectors.toMap(HealthIndicatorResult::name, i -> i))
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("status", status);
        builder.startObject("indicators");
        for (Map.Entry<String, HealthIndicatorResult> indicator : indicators.entrySet()) {
            builder.field(indicator.getKey());
            indicator.getValue().toXContent(builder, params);
        }
        builder.endObject();
        return builder.endObject();
    }
}
