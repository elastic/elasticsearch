/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle.health;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.Strings;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.node.DataStreamLifecycleHealthInfo;
import org.elasticsearch.health.node.DslErrorInfo;
import org.elasticsearch.health.node.FileSettingsHealthInfo;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.health.node.ProjectIndexName;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static org.elasticsearch.datastreams.lifecycle.health.DataStreamLifecycleHealthIndicatorService.STAGNATING_BACKING_INDICES_DIAGNOSIS_DEF;
import static org.elasticsearch.datastreams.lifecycle.health.DataStreamLifecycleHealthIndicatorService.STAGNATING_INDEX_IMPACT;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNot.not;

public class DataStreamLifecycleHealthIndicatorServiceTests extends ESTestCase {

    private boolean multiProject;
    private Set<ProjectId> projectIds;
    private DataStreamLifecycleHealthIndicatorService service;

    @Before
    public void chooseProjects() {
        multiProject = randomBoolean();
        projectIds = multiProject
            ? IntStream.range(0, randomIntBetween(1, 5)).mapToObj(i -> randomUniqueProjectId()).collect(Collectors.toSet())
            : Set.of(randomProjectIdOrDefault());
        service = new DataStreamLifecycleHealthIndicatorService(
            multiProject ? TestProjectResolvers.allProjects() : TestProjectResolvers.singleProjectOnly(projectIds.iterator().next())
        );
    }

    public void testGreenWhenNoDSLHealthData() {
        HealthIndicatorResult result = service.calculate(true, constructHealthInfo(null));
        assertThat(result.status(), is(HealthStatus.GREEN));
        assertThat(
            result.symptom(),
            is("No data stream lifecycle health data available yet. Health information will be reported after the first run.")
        );
        assertThat(result.details(), is(HealthIndicatorDetails.EMPTY));
        assertThat(result.impacts(), is(List.of()));
        assertThat(result.diagnosisList(), is(List.of()));
    }

    public void testGreenWhenEmptyListOfStagnatingIndices() {
        int totalBackingIndicesInError = 15 * projectIds.size();
        HealthIndicatorResult result = service.calculate(
            true,
            constructHealthInfo(new DataStreamLifecycleHealthInfo(List.of(), totalBackingIndicesInError))
        );
        assertThat(result.status(), is(HealthStatus.GREEN));
        assertThat(result.symptom(), is("Data streams are executing their lifecycles without issues"));
        assertThat(result.details(), is(not(HealthIndicatorDetails.EMPTY)));
        assertThat(Strings.toString(result.details()), containsString("\"total_backing_indices_in_error\":" + totalBackingIndicesInError));
        assertThat(result.impacts(), is(List.of()));
        assertThat(result.diagnosisList(), is(List.of()));
    }

    public void testYellowWhenStagnatingIndicesPresent() {
        String secondGenerationIndex = DataStream.getDefaultBackingIndexName("foo", 2L);
        String firstGenerationIndex = DataStream.getDefaultBackingIndexName("foo", 1L);
        int stagnatingCount = 2 * projectIds.size();
        int totalBackingIndicesInError = 15 * projectIds.size();
        HealthIndicatorResult result = service.calculate(
            true,
            constructHealthInfo(
                new DataStreamLifecycleHealthInfo(stagnatingErrors(secondGenerationIndex, firstGenerationIndex), totalBackingIndicesInError)
            )
        );
        assertThat(result.status(), is(HealthStatus.YELLOW));
        assertThat(
            result.symptom(),
            is(
                stagnatingCount == 1
                    ? "A backing index has repeatedly encountered errors whilst trying to advance in its lifecycle"
                    : stagnatingCount + " backing indices have repeatedly encountered errors whilst trying to advance in its lifecycle"
            )
        );
        assertThat(result.details(), is(not(HealthIndicatorDetails.EMPTY)));
        String detailsAsString = Strings.toString(result.details());
        assertThat(detailsAsString, containsString("\"total_backing_indices_in_error\":" + totalBackingIndicesInError));
        assertThat(detailsAsString, containsString("\"stagnating_backing_indices_count\":" + stagnatingCount));
        for (String displayName : stagnatingDisplayNames(secondGenerationIndex, firstGenerationIndex)) {
            assertThat(detailsAsString, containsString("\"index_name\":\"" + displayName + "\""));
        }
        assertThat(result.impacts(), is(STAGNATING_INDEX_IMPACT));
        Diagnosis diagnosis = result.diagnosisList().get(0);
        assertThat(diagnosis.definition(), is(STAGNATING_BACKING_INDICES_DIAGNOSIS_DEF));
        assertThat(
            diagnosis.affectedResources().get(0).getValues(),
            containsInAnyOrder(stagnatingDisplayNames(secondGenerationIndex, firstGenerationIndex).toArray())
        );
    }

    public void testSkippingFieldsWhenVerboseIsFalse() {
        String secondGenerationIndex = DataStream.getDefaultBackingIndexName("foo", 2L);
        String firstGenerationIndex = DataStream.getDefaultBackingIndexName("foo", 1L);
        int stagnatingCount = 2 * projectIds.size();
        HealthIndicatorResult result = service.calculate(
            false,
            constructHealthInfo(
                new DataStreamLifecycleHealthInfo(stagnatingErrors(secondGenerationIndex, firstGenerationIndex), 15 * projectIds.size())
            )
        );
        assertThat(result.status(), is(HealthStatus.YELLOW));
        assertThat(
            result.symptom(),
            is(
                stagnatingCount == 1
                    ? "A backing index has repeatedly encountered errors whilst trying to advance in its lifecycle"
                    : stagnatingCount + " backing indices have repeatedly encountered errors whilst trying to advance in its lifecycle"
            )
        );
        assertThat(result.details(), is(HealthIndicatorDetails.EMPTY));
        assertThat(result.impacts(), is(STAGNATING_INDEX_IMPACT));
        assertThat(result.diagnosisList().isEmpty(), is(true));
    }

    private List<DslErrorInfo> stagnatingErrors(String secondGenerationIndex, String firstGenerationIndex) {
        List<DslErrorInfo> errors = new ArrayList<>();
        for (ProjectId projectId : projectIds) {
            errors.add(new DslErrorInfo(secondGenerationIndex, 1L, 200, projectId));
            errors.add(new DslErrorInfo(firstGenerationIndex, 3L, 100, projectId));
        }
        return errors;
    }

    private List<String> stagnatingDisplayNames(String... indexNames) {
        List<String> names = new ArrayList<>();
        for (String indexName : indexNames) {
            for (ProjectId projectId : projectIds) {
                names.add(multiProject ? new ProjectIndexName(projectId, indexName).toString(true) : indexName);
            }
        }
        return names;
    }

    private HealthInfo constructHealthInfo(DataStreamLifecycleHealthInfo dslHealthInfo) {
        return new HealthInfo(Map.of(), dslHealthInfo, Map.of(), FileSettingsHealthInfo.INDETERMINATE);
    }
}
