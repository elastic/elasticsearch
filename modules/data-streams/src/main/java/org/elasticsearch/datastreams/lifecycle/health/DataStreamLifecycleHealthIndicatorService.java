/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle.health;

import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.node.DataStreamLifecycleHealthInfo;
import org.elasticsearch.health.node.DslErrorInfo;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.health.node.ProjectIndexName;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class DataStreamLifecycleHealthIndicatorService implements HealthIndicatorService {

    public static final String NAME = "data_stream_lifecycle";
    public static final String DSL_EXPLAIN_HELP_URL = "https://ela.st/explain-data-stream-lifecycle";

    public static final String STAGNATING_BACKING_INDEX_IMPACT_ID = "stagnating_backing_index";

    public static final List<HealthIndicatorImpact> STAGNATING_INDEX_IMPACT = List.of(
        new HealthIndicatorImpact(
            NAME,
            STAGNATING_BACKING_INDEX_IMPACT_ID,
            3,
            "Data streams backing indices cannot make progress in their lifecycle. The performance and "
                + "stability of the indices and/or the cluster could be impacted.",
            List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)
        )
    );

    public static final Diagnosis.Definition STAGNATING_BACKING_INDICES_DIAGNOSIS_DEF = new Diagnosis.Definition(
        NAME,
        "stagnating_dsl_backing_index",
        "Some backing indices are repeatedly encountering errors in their lifecycle execution.",
        "Check the current status of the affected indices using the [GET /<affected_index_name>/_lifecycle/explain] API. Please "
            + "replace the <affected_index_name> in the API with the actual index name (or the data stream name for a wider overview).",
        DSL_EXPLAIN_HELP_URL
    );

    private final ProjectResolver projectResolver;

    public DataStreamLifecycleHealthIndicatorService(ProjectResolver projectResolver) {
        this.projectResolver = projectResolver;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public HealthIndicatorResult calculate(boolean verbose, int maxAffectedResourcesCount, HealthInfo healthInfo) {
        DataStreamLifecycleHealthInfo dataStreamLifecycleHealthInfo = healthInfo.dslHealthInfo();
        if (dataStreamLifecycleHealthInfo == null) {
            // DSL reports health information on every run, so data will eventually arrive to the health node. In the meantime, let's
            // report GREEN health, as there are no errors to report before the first run anyway.
            return createIndicator(
                HealthStatus.GREEN,
                "No data stream lifecycle health data available yet. Health information will be reported after the first run.",
                HealthIndicatorDetails.EMPTY,
                List.of(),
                List.of()
            );
        }

        List<DslErrorInfo> stagnatingBackingIndices = dataStreamLifecycleHealthInfo.dslErrorsInfo();
        if (stagnatingBackingIndices.isEmpty()) {
            return createIndicator(
                HealthStatus.GREEN,
                "Data streams are executing their lifecycles without issues",
                createDetails(verbose, dataStreamLifecycleHealthInfo, projectResolver.supportsMultipleProjects()),
                List.of(),
                List.of()
            );
        } else {
            List<String> affectedIndices = stagnatingBackingIndices.stream()
                .map(dslErrorInfo -> indexDisplayName(dslErrorInfo, projectResolver.supportsMultipleProjects()))
                .limit(Math.min(maxAffectedResourcesCount, stagnatingBackingIndices.size()))
                .collect(toList());
            return createIndicator(
                HealthStatus.YELLOW,
                (stagnatingBackingIndices.size() > 1 ? stagnatingBackingIndices.size() + " backing indices have" : "A backing index has")
                    + " repeatedly encountered errors whilst trying to advance in its lifecycle",
                createDetails(verbose, dataStreamLifecycleHealthInfo, projectResolver.supportsMultipleProjects()),
                STAGNATING_INDEX_IMPACT,
                verbose
                    ? List.of(
                        new Diagnosis(
                            STAGNATING_BACKING_INDICES_DIAGNOSIS_DEF,
                            List.of(new Diagnosis.Resource(Diagnosis.Resource.Type.INDEX, affectedIndices))
                        )
                    )
                    : List.of()
            );
        }
    }

    private static HealthIndicatorDetails createDetails(
        boolean verbose,
        DataStreamLifecycleHealthInfo dataStreamLifecycleHealthInfo,
        boolean supportsMultipleProjects
    ) {
        if (verbose == false) {
            return HealthIndicatorDetails.EMPTY;
        }

        var details = new HashMap<String, Object>();
        details.put("total_backing_indices_in_error", dataStreamLifecycleHealthInfo.totalErrorEntriesCount());
        details.put("stagnating_backing_indices_count", dataStreamLifecycleHealthInfo.dslErrorsInfo().size());
        if (dataStreamLifecycleHealthInfo.dslErrorsInfo().isEmpty() == false) {
            details.put("stagnating_backing_indices", dataStreamLifecycleHealthInfo.dslErrorsInfo().stream().map(dslError -> {
                LinkedHashMap<String, Object> errorDetails = new LinkedHashMap<>(3, 1L);
                errorDetails.put("index_name", indexDisplayName(dslError, supportsMultipleProjects));
                errorDetails.put("first_occurrence_timestamp", dslError.firstOccurrence());
                errorDetails.put("retry_count", dslError.retryCount());
                return errorDetails;
            }).toList());
        }
        return new SimpleHealthIndicatorDetails(details);
    }

    private static String indexDisplayName(DslErrorInfo dslErrorInfo, boolean supportsMultipleProjects) {
        return new ProjectIndexName(dslErrorInfo.projectId(), dslErrorInfo.indexName()).toString(supportsMultipleProjects);
    }
}
