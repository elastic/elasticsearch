/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.gateway.GatewayService;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Predicate.isEqual;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * This service collects health indicators from all modules and plugins of elasticsearch
 */
public class HealthService {

    // Visible for testing
    static final String UNKNOWN_RESULT_SUMMARY_PREFLIGHT_FAILED = "Could not determine indicator state. Cluster state is not stable. Check "
        + "details for critical issues keeping this indicator from running.";
    static final String UNKNOWN_RESULT_SUMMARY_NOT_RECOVERED =
        "Could not determine indicator state. The current node handling the health request is not ready to assess the health of the "
            + "cluster. Try again later or execute the health API against a different node.";

    /**
     * Detail map key that contains the reasons a result was marked as UNKNOWN
     */
    private static final String REASON = "reasons";

    private static final String CLUSTER_STATE_RECOVERED = "cluster_state_recovered";
    private static final SimpleHealthIndicatorDetails DETAILS_UNKNOWN_STATE_NOT_RECOVERED = new SimpleHealthIndicatorDetails(
        Map.of(REASON, Map.of(CLUSTER_STATE_RECOVERED, false))
    );

    private final List<HealthIndicatorService> preflightHealthIndicatorServices;
    private final List<HealthIndicatorService> healthIndicatorServices;
    private final ClusterService clusterService;

    /**
     * Creates a new HealthService.
     *
     * Accepts a list of regular indicator services and a list of preflight indicator services. Preflight indicators are run first and
     * represent serious cascading health problems. If any of these preflight indicators are not GREEN status, all remaining indicators are
     * likely to be degraded in some way or will not be able to calculate their state correctly. The remaining health indicators will return
     * UNKNOWN statuses in this case.
     *
     * @param preflightHealthIndicatorServices indicators that are run first and represent a serious cascading health problem.
     * @param healthIndicatorServices indicators that are run if the preflight indicators return GREEN results.
     */
    public HealthService(
        List<HealthIndicatorService> preflightHealthIndicatorServices,
        List<HealthIndicatorService> healthIndicatorServices,
        ClusterService clusterService
    ) {
        this.preflightHealthIndicatorServices = preflightHealthIndicatorServices;
        this.healthIndicatorServices = healthIndicatorServices;
        this.clusterService = clusterService;
    }

    /**
     * Returns the list of HealthComponentResults for this cluster. If no componentName is specified, one HealthComponentResult is returned
     * for each component in the system. If a componentName is given, only the single HealthComponentResult for that component is
     * returned. If both a componentName and indicatorName are given, the returned HealthComponentResult will only have information about
     * the given indicatorName.
     * @param componentName If not null, only the component with this name is returned
     * @param indicatorName If not null, the returned component will only have this indicator
     * @param explain Whether to compute the details portion of the component results
     * @return A list of all HealthComponentResults if componentName is null, or one HealthComponentResult if componentName is not null
     * @throws ResourceNotFoundException if a component name is given and the component or indicator are not found
     */
    public List<HealthComponentResult> getHealth(@Nullable String componentName, @Nullable String indicatorName, boolean explain) {
        final boolean shouldDrillDownToIndicatorLevel = indicatorName != null;
        final boolean showRolledUpComponentStatus = shouldDrillDownToIndicatorLevel == false;

        // Is the cluster state recovered? If not, ALL indicators should return UNKNOWN
        boolean clusterStateRecovered = clusterService.state()
            .getBlocks()
            .hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK) == false;

        List<HealthIndicatorResult> preflightResults;
        if (clusterStateRecovered) {
            // Determine if cluster is stable enough to calculate health before running other indicators
            preflightResults = preflightHealthIndicatorServices.stream().map(service -> service.calculate(explain)).toList();
        } else {
            // Mark preflight indicators as UNKNOWN
            HealthIndicatorDetails details = explain ? DETAILS_UNKNOWN_STATE_NOT_RECOVERED : HealthIndicatorDetails.EMPTY;
            preflightResults = preflightHealthIndicatorServices.stream()
                .map(service -> generateUnknownResult(service, UNKNOWN_RESULT_SUMMARY_NOT_RECOVERED, details))
                .toList();
        }

        // If any of these are not GREEN, then we cannot obtain health from other indicators
        boolean clusterHealthIsObtainable = preflightResults.isEmpty()
            || preflightResults.stream().map(HealthIndicatorResult::status).allMatch(isEqual(HealthStatus.GREEN));

        // Filter remaining indicators by component name and indicator name if present before calculating their results
        Stream<HealthIndicatorService> filteredIndicators = healthIndicatorServices.stream()
            .filter(service -> componentName == null || service.component().equals(componentName))
            .filter(service -> indicatorName == null || service.name().equals(indicatorName));

        Stream<HealthIndicatorResult> filteredIndicatorResults;
        if (clusterStateRecovered && clusterHealthIsObtainable) {
            // Calculate remaining indicators
            filteredIndicatorResults = filteredIndicators.map(service -> service.calculate(explain));
        } else {
            // Mark remaining indicators as UNKNOWN
            String unknownSummary = clusterStateRecovered ? UNKNOWN_RESULT_SUMMARY_PREFLIGHT_FAILED : UNKNOWN_RESULT_SUMMARY_NOT_RECOVERED;
            HealthIndicatorDetails unknownDetails = healthUnknownReason(preflightResults, clusterStateRecovered, explain);
            filteredIndicatorResults = filteredIndicators.map(service -> generateUnknownResult(service, unknownSummary, unknownDetails));
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
    private HealthIndicatorDetails healthUnknownReason(
        List<HealthIndicatorResult> preflightResults,
        boolean clusterStateRecovered,
        boolean computeDetails
    ) {
        assert clusterStateRecovered == false || preflightResults.isEmpty() == false
            : "Requires at least one non-GREEN preflight result or cluster state not recovered";
        HealthIndicatorDetails unknownDetails;
        if (computeDetails) {
            if (clusterStateRecovered) {
                // Determine why the cluster is not stable enough for running remaining indicators
                Map<String, String> clusterUnstableReasons = preflightResults.stream()
                    .filter(result -> HealthStatus.GREEN.equals(result.status()) == false)
                    .collect(toMap(HealthIndicatorResult::name, result -> result.status().xContentValue()));
                assert clusterUnstableReasons.isEmpty() == false : "Requires at least one non-GREEN preflight result";
                unknownDetails = new SimpleHealthIndicatorDetails(Map.of(REASON, clusterUnstableReasons));
            } else {
                unknownDetails = DETAILS_UNKNOWN_STATE_NOT_RECOVERED;
            }
        } else {
            unknownDetails = HealthIndicatorDetails.EMPTY;
        }
        return unknownDetails;
    }

    /**
     * Generates an UNKNOWN result for an indicator
     * @param indicatorService the indicator to generate a result for
     * @param summary the summary to include for the UNKNOWN result
     * @param details the details to include on the result
     * @return A result with the UNKNOWN status
     */
    private HealthIndicatorResult generateUnknownResult(
        HealthIndicatorService indicatorService,
        String summary,
        HealthIndicatorDetails details
    ) {
        return indicatorService.createIndicator(HealthStatus.UNKNOWN, summary, details, Collections.emptyList(), Collections.emptyList());
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
