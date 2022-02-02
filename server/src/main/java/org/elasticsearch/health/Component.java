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
import java.util.TreeMap;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

public record Component(String name, HealthStatus status, List<HealthIndicator> indicators) implements ToXContentObject {

    public static Collection<Component> createComponentsFromIndicators(Collection<HealthIndicator> indicators) {
        return indicators.stream()
            .collect(
                groupingBy(HealthIndicator::component, TreeMap::new, collectingAndThen(toList(), Component::createComponentFromIndicators))
            )
            .values();
    }

    private static Component createComponentFromIndicators(List<HealthIndicator> indicators) {
        assert indicators.size() > 0 : "Component should not be non empty";
        assert indicators.stream().map(HealthIndicator::component).distinct().count() == 1L
            : "Should not mix indicators from different components";
        return new Component(
            indicators.get(0).component(),
            HealthStatus.merge(indicators.stream().map(HealthIndicator::status)),
            indicators
        );
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
