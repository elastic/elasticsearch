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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SimpleHealthIndicator implements HealthIndicatorService {

    private final String name;
    private final String greenSymptom;
    private final String yellowSymptom;
    private final String redSymptom;

    public SimpleHealthIndicator(String name, String greenSymptom, String yellowSymptom, String redSymptom) {
        this.name = name;
        this.greenSymptom = greenSymptom;
        this.yellowSymptom = yellowSymptom;
        this.redSymptom = redSymptom;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public HealthIndicatorResult calculate(boolean verbose, int maxAffectedResourcesCount, HealthInfo healthInfo) {
        List<SimpleNodeHealthInfo> simpleNodeHealthInfo = healthInfo.simpleNodeHealthInfoByMonitor().get(name);

        if (simpleNodeHealthInfo == null || simpleNodeHealthInfo.isEmpty()) {
            return createIndicator(HealthStatus.UNKNOWN, "No data reported", null, Collections.emptyList(), Collections.emptyList());
        }

        Map<String, Object> detailsByNode = new HashMap<>();
        for (SimpleNodeHealthInfo nodeHealthInfo : simpleNodeHealthInfo) {
            if (nodeHealthInfo.details() != null && nodeHealthInfo.details().isEmpty() == false) {
                detailsByNode.put(nodeHealthInfo.node(), nodeHealthInfo.details());
            }
        }

        String nodeList = simpleNodeHealthInfo.stream().map(SimpleNodeHealthInfo::node).collect(Collectors.joining(", "));

        HealthStatus overallStatus = HealthStatus.merge(simpleNodeHealthInfo.stream().map(SimpleNodeHealthInfo::healthStatus));

        return switch (overallStatus) {
            case GREEN -> createIndicator(
                HealthStatus.GREEN,
                greenSymptom,
                null,
                Collections.emptyList(),
                Collections.emptyList()
            );
            case YELLOW -> createIndicator(
                HealthStatus.YELLOW,
                yellowSymptom + " on the following nodes: " + nodeList,
                new SimpleHealthIndicatorDetails(detailsByNode),
                Collections.emptyList(),
                Collections.emptyList()
            );
            case RED -> createIndicator(
                HealthStatus.RED,
                redSymptom + " on the following nodes: " + nodeList,
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
