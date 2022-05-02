/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.core.Nullable;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * This service collects health indicators from all modules and plugins of elasticsearch
 */
public class HealthService {

    private final List<HealthIndicatorService> preflightHealthIndicatorServices;
    private final List<HealthIndicatorService> healthIndicatorServices;

    /**
     * @param preflightHealthIndicatorServices indicators that are run first and block health calculation until GREEN.
     * @param healthIndicatorServices indicators that are run if the cluster-stable indicators return GREEN results.
     */
    public HealthService(
        List<HealthIndicatorService> preflightHealthIndicatorServices,
        List<HealthIndicatorService> healthIndicatorServices
    ) {
        this.preflightHealthIndicatorServices = preflightHealthIndicatorServices;
        this.healthIndicatorServices = healthIndicatorServices;
    }

    /**
     * Returns the list of HealthComponentResults for this cluster. If no componentName is specified, one HealthComponentResult is returned
     * for each component in the system. If a componentName is given, only the single HealthComponentResult for that component is
     * returned. If both a componentName and indicatorName are given, the returned HealthComponentResult will only have information about
     * the given indicatorName.
     * @param componentName If not null, only the component with this name is returned
     * @param indicatorName If not null, the returned component will only have this indicator
     * @param computeDetails Whether to compute the details portion of the component results
     * @return A list of all HealthComponentResults if componentName is null, or one HealthComponentResult if componentName is not null
     * @throws ResourceNotFoundException if a component name is given and the component or indicator are not found
     */
    public List<HealthComponentResult> getHealth(@Nullable String componentName, @Nullable String indicatorName, boolean computeDetails) {
        final boolean shouldDrillDownToIndicatorLevel = indicatorName != null;
        final boolean showRolledUpComponentStatus = shouldDrillDownToIndicatorLevel == false;

        // Determine if cluster is stable enough to determine health before running other indicators
        List<HealthIndicatorResult> preflightResults = preflightHealthIndicatorServices.stream()
            .map(service -> service.calculate(computeDetails))
            .toList();

        // If any of these are not GREEN, then we cannot obtain health from other indicators
        boolean clusterHealthIsObtainable = preflightResults.isEmpty() || preflightResults.stream()
            .anyMatch(result -> HealthStatus.GREEN.equals(result.status()) == false);

        // Filter indicators by component name and indicator name if present before calculating their results
        Stream<HealthIndicatorService> filteredIndicators = healthIndicatorServices.stream()
            .filter(service -> componentName == null || service.component().equals(componentName))
            .filter(service -> indicatorName == null || service.name().equals(indicatorName));

        Stream<HealthIndicatorResult> filteredIndicatorResults;
        if (clusterHealthIsObtainable) {
            // Calculate remaining indicators
            filteredIndicatorResults = filteredIndicators.map(indicatorService -> indicatorService.calculate(computeDetails));
        } else {
            // Mark remaining indicators as UNKNOWN
            HealthIndicatorDetails unknownDetails = determineHealthUnknownReason(preflightResults, computeDetails);
            filteredIndicatorResults = filteredIndicators.map(indicatorService ->
                indicatorService.createIndicator(
                    HealthStatus.UNKNOWN,
                    "Could not determine indicator state",
                    unknownDetails,
                    Collections.emptyList()
                )
            );
        }

        // Filter the cluster indicator results by component name and indicator name if present
        Stream<HealthIndicatorResult> filteredPreflightResults = preflightResults.stream()
            .filter(result -> componentName == null || result.component().equals(componentName))
            .filter(result -> indicatorName == null || result.name().equals(indicatorName));

        // Combine indicator results
        List<HealthComponentResult> components = List.copyOf(
            Stream.concat(filteredPreflightResults, filteredIndicatorResults)
                .collect(
                    groupingBy(
                        HealthIndicatorResult::component,
                        TreeMap::new,
                        collectingAndThen(
                            toList(),
                            indicators -> HealthService.createComponentFromIndicators(indicators, showRolledUpComponentStatus)
                        )
                    )
                )
                .values()
        );
        if (components.isEmpty() && componentName != null) {
            String errorMessage;
            if (indicatorName != null) {
                errorMessage = String.format(Locale.ROOT, "Did not find indicator %s in component %s", indicatorName, componentName);
            } else {
                errorMessage = String.format(Locale.ROOT, "Did not find component %s", componentName);
            }
            throw new ResourceNotFoundException(errorMessage);
        }
        return components;
    }

    /**
     * Return details to include on health indicator results when health information cannot be obtained due to unstable cluster.
     * @param preflightResults Results of indicators used to determine if health checks can happen.
     * @param computeDetails If details should be calculated on which indicators are causing the UNKNOWN state.
     * @return Details explaining why results are UNKNOWN, or an empty detail set if computeDetails is false.
     */
    private HealthIndicatorDetails determineHealthUnknownReason(List<HealthIndicatorResult> preflightResults, boolean computeDetails) {
        assert preflightResults.isEmpty() == false : "Requires at least one non-GREEN preflight result";
        HealthIndicatorDetails unknownDetails;
        if (computeDetails) {
            // Determine why the cluster is not stable enough for running remaining indicators
            Map<String, String> clusterUnstableReasons = preflightResults.stream()
                .filter(result -> HealthStatus.GREEN.equals(result.status()) == false)
                .collect(toMap(HealthIndicatorResult::name, result -> result.status().xContentValue()));
            assert clusterUnstableReasons.isEmpty() == false : "Requires at least one non-GREEN preflight result";
            unknownDetails = new SimpleHealthIndicatorDetails(Map.of("reasons", clusterUnstableReasons));
        } else {
            unknownDetails = HealthIndicatorDetails.EMPTY;
        }
        return unknownDetails;
    }

    // Non-private for testing purposes
    static HealthComponentResult createComponentFromIndicators(List<HealthIndicatorResult> indicators, boolean showComponentSummary) {
        assert indicators.size() > 0 : "Component should not be non empty";
        assert indicators.stream().map(HealthIndicatorResult::component).distinct().count() == 1L
            : "Should not mix indicators from different components";
        assert findDuplicatesByName(indicators).isEmpty()
            : String.format(
                Locale.ROOT,
                "Found multiple indicators with the same name within the %s component: %s",
                indicators.get(0).component(),
                findDuplicatesByName(indicators)
            );
        return new HealthComponentResult(
            indicators.get(0).component(),
            showComponentSummary ? HealthStatus.merge(indicators.stream().map(HealthIndicatorResult::status)) : null,
            indicators
        );
    }

    private static Set<String> findDuplicatesByName(List<HealthIndicatorResult> indicators) {
        Set<String> items = new HashSet<>();
        return indicators.stream().map(HealthIndicatorResult::name).filter(name -> items.add(name) == false).collect(Collectors.toSet());
    }
}
