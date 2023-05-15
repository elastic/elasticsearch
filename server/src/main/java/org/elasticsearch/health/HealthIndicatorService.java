/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.health.node.HealthInfo;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This is a service interface used to calculate health indicator from the different modules or plugins.

 * NOTE: if you are adding the name of an indicator or the id of a diagnosis you need to update the configuration
 * of the health-api-indexer in the telemetry repository so the new/changed fields will be indexed properly.
 */
public interface HealthIndicatorService {

    int MAX_AFFECTED_RESOURCES_COUNT = 1000;

    String name();

    default HealthIndicatorResult calculate(boolean verbose, HealthInfo healthInfo) {
        return calculate(verbose, MAX_AFFECTED_RESOURCES_COUNT, healthInfo);
    }

    HealthIndicatorResult calculate(boolean verbose, int maxAffectedResourcesCount, HealthInfo healthInfo);

    /**
     * This method creates a HealthIndicatorResult with the given information. Note that it sorts the impacts by severity (the lower the
     * number the higher the severity), and only keeps the 3 highest-severity impacts.
     * @param status The status of the result
     * @param symptom The symptom used in the result
     * @param details The details used in the result
     * @param impacts A collection of impacts. Only the 3 highest severity impacts are used in the result
     * @return A HealthIndicatorResult built from the given information
     */
    default HealthIndicatorResult createIndicator(
        HealthStatus status,
        String symptom,
        HealthIndicatorDetails details,
        Collection<HealthIndicatorImpact> impacts,
        List<Diagnosis> diagnosisList
    ) {
        List<HealthIndicatorImpact> impactsList = impacts.stream()
            .sorted(Comparator.comparingInt(HealthIndicatorImpact::severity))
            .limit(3)
            .collect(Collectors.toList());
        return new HealthIndicatorResult(name(), status, symptom, details, impactsList, diagnosisList);
    }
}
