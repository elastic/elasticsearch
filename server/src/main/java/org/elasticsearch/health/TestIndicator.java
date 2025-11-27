/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health;

import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.health.node.SimpleNodeHealthInfo;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TestIndicator implements HealthIndicatorService {
    @Override
    public String name() {
        return "test-tracker";
    }

    @Override
    public HealthIndicatorResult calculate(boolean verbose, int maxAffectedResourcesCount, HealthInfo healthInfo) {
        List<SimpleNodeHealthInfo> simpleNodeHealthInfo = healthInfo.simpleNodeHealthInfoByMonitor().get("test-tracker");

        if (simpleNodeHealthInfo.isEmpty()) {
            return createIndicator(HealthStatus.UNKNOWN, "No data reported", null, Collections.emptyList(), Collections.emptyList());
        }

        Map<String, Object> detailsByNode = simpleNodeHealthInfo.stream()
            .filter(shi -> shi.healthStatus().indicatesHealthProblem())
            .collect(java.util.stream.Collectors.toMap(SimpleNodeHealthInfo::node, SimpleNodeHealthInfo::details));

        String nodeList = detailsByNode.keySet().stream().collect(Collectors.joining(", "));

        HealthStatus overallStatus = HealthStatus.merge(simpleNodeHealthInfo.stream().map(SimpleNodeHealthInfo::healthStatus));

        return switch (overallStatus) {
            case GREEN -> createIndicator(
                HealthStatus.GREEN,
                "All nodes are reporting healthy",
                null,
                Collections.emptyList(),
                Collections.emptyList()
            );
            case YELLOW -> createIndicator(
                HealthStatus.YELLOW,
                "Some nodes are experiencing issues: " + nodeList,
                new SimpleHealthIndicatorDetails(detailsByNode),
                Collections.emptyList(),
                Collections.emptyList()
            );
            case RED -> createIndicator(
                HealthStatus.RED,
                "Some nodes are experiencing critical issues: " + nodeList,
                new SimpleHealthIndicatorDetails(detailsByNode),
                Collections.emptyList(),
                Collections.emptyList()
            );
            case UNKNOWN -> createIndicator(
                HealthStatus.UNKNOWN,
                "Some nodes are unable to report their status: " + nodeList,
                new SimpleHealthIndicatorDetails(detailsByNode),
                Collections.emptyList(),
                Collections.emptyList()
            );
        };
    }
}
