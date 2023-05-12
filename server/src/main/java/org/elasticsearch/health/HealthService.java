/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.health.node.FetchHealthInfoCacheAction;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Predicate.isEqual;
import static java.util.stream.Collectors.toMap;

/**
 * This service collects health indicators from all modules and plugins of elasticsearch
 */
public class HealthService {

    // Visible for testing
    static final String UNKNOWN_RESULT_SUMMARY_PREFLIGHT_FAILED = "Could not determine health status. Check details on critical issues "
        + "preventing the health status from reporting.";

    public static final String HEALTH_API_ID_PREFIX = "elasticsearch:health:";

    /**
     * Detail map key that contains the reasons a result was marked as UNKNOWN
     */
    private static final String REASON = "reasons";

    private final List<HealthIndicatorService> preflightHealthIndicatorServices;
    private final List<HealthIndicatorService> healthIndicatorServices;
    private final ThreadPool threadPool;

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
        ThreadPool threadPool
    ) {
        this.preflightHealthIndicatorServices = preflightHealthIndicatorServices;
        this.healthIndicatorServices = healthIndicatorServices;
        this.threadPool = threadPool;
    }

    /**
     * Returns the list of HealthIndicatorResult for this cluster.
     *
     * @param client        A client to be used to fetch the health data from the health node
     * @param indicatorName If not null, the returned results will only have this indicator
     * @param verbose       Whether to compute the details portion of the results
     * @param listener      A listener to be notified of the list of all HealthIndicatorResult if indicatorName is null, or one
     *                      HealthIndicatorResult if indicatorName is not null
     * @param maxAffectedResourcesCount The maximum number of affected resources to return per each type.
     * @throws ResourceNotFoundException if an indicator name is given and the indicator is not found
     */
    public void getHealth(
        Client client,
        @Nullable String indicatorName,
        boolean verbose,
        int maxAffectedResourcesCount,
        ActionListener<List<HealthIndicatorResult>> listener
    ) {
        if (maxAffectedResourcesCount < 0) {
            throw new IllegalArgumentException("The max number of resources must be a positive integer");
        }
        // Determine if cluster is stable enough to calculate health before running other indicators
        List<HealthIndicatorResult> preflightResults = preflightHealthIndicatorServices.stream()
            .map(service -> service.calculate(verbose, maxAffectedResourcesCount, HealthInfo.EMPTY_HEALTH_INFO))
            .toList();

        // If any of these are not GREEN, then we cannot obtain health from other indicators
        boolean clusterHealthIsObtainable = preflightResults.isEmpty()
            || preflightResults.stream().map(HealthIndicatorResult::status).allMatch(isEqual(HealthStatus.GREEN));

        // Filter remaining indicators by indicator name if present before calculating their results
        Stream<HealthIndicatorService> filteredIndicators = healthIndicatorServices.stream()
            .filter(service -> indicatorName == null || service.name().equals(indicatorName));
        Stream<HealthIndicatorResult> filteredPreflightResults = preflightResults.stream()
            .filter(result -> indicatorName == null || result.name().equals(indicatorName));

        if (clusterHealthIsObtainable) {

            client.execute(FetchHealthInfoCacheAction.INSTANCE, new FetchHealthInfoCacheAction.Request(), new ActionListener<>() {
                @Override
                public void onResponse(FetchHealthInfoCacheAction.Response response) {
                    HealthInfo healthInfo = response.getHealthInfo();
                    // fork off to the management pool as calculating the indicators can run for longer than is acceptable
                    // on a transport thread in case of large numbers of indices
                    ActionRunnable<List<HealthIndicatorResult>> calculateFilteredIndicatorsRunnable = calculateFilteredIndicatorsRunnable(
                        indicatorName,
                        healthInfo,
                        verbose,
                        listener
                    );

                    try {
                        threadPool.executor(ThreadPool.Names.MANAGEMENT).submit(calculateFilteredIndicatorsRunnable);
                    } catch (EsRejectedExecutionException e) {
                        calculateFilteredIndicatorsRunnable.onRejection(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    // fork off to the management pool as calculating the indicators can run for longer than is acceptable
                    // on a transport thread in case of large numbers of indices
                    ActionRunnable<List<HealthIndicatorResult>> calculateFilteredIndicatorsRunnable = calculateFilteredIndicatorsRunnable(
                        indicatorName,
                        HealthInfo.EMPTY_HEALTH_INFO,
                        verbose,
                        listener
                    );
                    try {
                        threadPool.executor(ThreadPool.Names.MANAGEMENT).submit(calculateFilteredIndicatorsRunnable);
                    } catch (EsRejectedExecutionException esRejectedExecutionException) {
                        calculateFilteredIndicatorsRunnable.onRejection(esRejectedExecutionException);
                    }
                }

                private ActionRunnable<List<HealthIndicatorResult>> calculateFilteredIndicatorsRunnable(
                    String indicatorName,
                    HealthInfo healthInfo,
                    boolean explain,
                    ActionListener<List<HealthIndicatorResult>> listener
                ) {
                    return ActionRunnable.wrap(listener, l -> {
                        List<HealthIndicatorResult> results = Stream.concat(
                            filteredPreflightResults,
                            filteredIndicators.map(service -> service.calculate(explain, maxAffectedResourcesCount, healthInfo))
                        ).toList();

                        validateResultsAndNotifyListener(indicatorName, results, l);
                    });
                }
            });

        } else {
            // Mark remaining indicators as UNKNOWN
            HealthIndicatorDetails unknownDetails = healthUnknownReason(preflightResults, verbose);
            Stream<HealthIndicatorResult> filteredIndicatorResults = filteredIndicators.map(
                service -> generateUnknownResult(service, UNKNOWN_RESULT_SUMMARY_PREFLIGHT_FAILED, unknownDetails)
            );
            validateResultsAndNotifyListener(
                indicatorName,
                Stream.concat(filteredPreflightResults, filteredIndicatorResults).toList(),
                listener
            );
        }
    }

    /**
     * This method validates the health indicator results, and notifies the listener. If assertions are enabled and there are indicators
     * with duplicate names, an AssertionError is thrown (the listener is not notified). If there are no results and the indicator name is
     * not null, the listener will be notified of failure because the user could not get the results that were asked for. Otherwise, the
     * listener will be notified with the results.
     *
     * @param indicatorName            If not null, the results will be validated to only have this indicator name
     * @param results                  The results that the listener will be notified of, if they pass validation
     * @param listener                 A listener to be notified of results
     */
    private void validateResultsAndNotifyListener(
        @Nullable String indicatorName,
        List<HealthIndicatorResult> results,
        ActionListener<List<HealthIndicatorResult>> listener
    ) {
        assert findDuplicatesByName(results).isEmpty()
            : String.format(Locale.ROOT, "Found multiple indicators with the same name: %s", findDuplicatesByName(results));
        if (results.isEmpty() && indicatorName != null) {
            String errorMessage = String.format(Locale.ROOT, "Did not find indicator %s", indicatorName);
            listener.onFailure(new ResourceNotFoundException(errorMessage));
        } else {
            listener.onResponse(results);
        }
    }

    /**
     * Return details to include on health indicator results when health information cannot be obtained due to unstable cluster.
     * @param preflightResults Results of indicators used to determine if health checks can happen.
     * @param computeDetails If details should be calculated on which indicators are causing the UNKNOWN state.
     * @return Details explaining why results are UNKNOWN, or an empty detail set if computeDetails is false.
     */
    private HealthIndicatorDetails healthUnknownReason(List<HealthIndicatorResult> preflightResults, boolean computeDetails) {
        assert preflightResults.isEmpty() == false : "Requires at least one non-GREEN preflight result";
        HealthIndicatorDetails unknownDetails;
        if (computeDetails) {
            // Determine why the cluster is not stable enough for running remaining indicators
            Map<String, String> clusterUnstableReasons = preflightResults.stream()
                .filter(result -> HealthStatus.GREEN.equals(result.status()) == false)
                .collect(toMap(HealthIndicatorResult::name, result -> result.status().xContentValue()));
            assert clusterUnstableReasons.isEmpty() == false : "Requires at least one non-GREEN preflight result";
            unknownDetails = new SimpleHealthIndicatorDetails(Map.of(REASON, clusterUnstableReasons));
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

    private static Set<String> findDuplicatesByName(List<HealthIndicatorResult> indicators) {
        Set<String> items = new HashSet<>();
        return indicators.stream().map(HealthIndicatorResult::name).filter(name -> items.add(name) == false).collect(Collectors.toSet());
    }
}
