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
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.health.node.ProjectIndexName;
import org.elasticsearch.reservedstate.service.FileSettingsService.FileSettingsHealthInfo;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.datastreams.lifecycle.health.DataStreamLifecycleHealthIndicatorService.STAGNATING_BACKING_INDICES_DIAGNOSIS_DEF;
import static org.elasticsearch.datastreams.lifecycle.health.DataStreamLifecycleHealthIndicatorService.STAGNATING_INDEX_IMPACT;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNot.not;

public class DataStreamLifecycleHealthIndicatorServiceTests extends ESTestCase {

    private DataStreamLifecycleHealthIndicatorService service;

    @Before
    public void setupService() {
        service = new DataStreamLifecycleHealthIndicatorService(TestProjectResolvers.singleProjectOnly(randomProjectIdOrDefault()));
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
        HealthIndicatorResult result = service.calculate(true, constructHealthInfo(new DataStreamLifecycleHealthInfo(List.of(), 15)));
        assertThat(result.status(), is(HealthStatus.GREEN));
        assertThat(result.symptom(), is("Data streams are executing their lifecycles without issues"));
        assertThat(result.details(), is(not(HealthIndicatorDetails.EMPTY)));
        assertThat(Strings.toString(result.details()), containsString("\"total_backing_indices_in_error\":15"));
        assertThat(result.impacts(), is(List.of()));
        assertThat(result.diagnosisList(), is(List.of()));
    }

    public void testYellowWhenStagnatingIndicesPresent() {
        String secondGenerationIndex = DataStream.getDefaultBackingIndexName("foo", 2L);
        String firstGenerationIndex = DataStream.getDefaultBackingIndexName("foo", 1L);
        HealthIndicatorResult result = service.calculate(
            true,
            constructHealthInfo(
                new DataStreamLifecycleHealthInfo(
                    List.of(new DslErrorInfo(secondGenerationIndex, 1L, 200), new DslErrorInfo(firstGenerationIndex, 3L, 100)),
                    15
                )
            )
        );
        assertThat(result.status(), is(HealthStatus.YELLOW));
        assertThat(result.symptom(), is("2 backing indices have repeatedly encountered errors whilst trying to advance in its lifecycle"));
        assertThat(result.details(), is(not(HealthIndicatorDetails.EMPTY)));
        String detailsAsString = Strings.toString(result.details());
        assertThat(detailsAsString, containsString("\"total_backing_indices_in_error\":15"));
        assertThat(detailsAsString, containsString("\"stagnating_backing_indices_count\":2"));
        assertThat(
            detailsAsString,
            containsString(
                String.format(
                    Locale.ROOT,
                    "\"index_name\":\"%s\","
                        + "\"first_occurrence_timestamp\":1,\"retry_count\":200},{\"index_name\":\"%s\","
                        + "\"first_occurrence_timestamp\":3,\"retry_count\":100",
                    secondGenerationIndex,
                    firstGenerationIndex
                )
            )
        );
        assertThat(result.impacts(), is(STAGNATING_INDEX_IMPACT));
        Diagnosis diagnosis = result.diagnosisList().get(0);
        assertThat(diagnosis.definition(), is(STAGNATING_BACKING_INDICES_DIAGNOSIS_DEF));
        assertThat(diagnosis.affectedResources().get(0).getValues(), containsInAnyOrder(secondGenerationIndex, firstGenerationIndex));
    }

    public void testSkippingFieldsWhenVerboseIsFalse() {
        String secondGenerationIndex = DataStream.getDefaultBackingIndexName("foo", 2L);
        String firstGenerationIndex = DataStream.getDefaultBackingIndexName("foo", 1L);
        HealthIndicatorResult result = service.calculate(
            false,
            constructHealthInfo(
                new DataStreamLifecycleHealthInfo(
                    List.of(new DslErrorInfo(secondGenerationIndex, 1L, 200), new DslErrorInfo(firstGenerationIndex, 3L, 100)),
                    15
                )
            )
        );
        assertThat(result.status(), is(HealthStatus.YELLOW));
        assertThat(result.symptom(), is("2 backing indices have repeatedly encountered errors whilst trying to advance in its lifecycle"));
        assertThat(result.details(), is(HealthIndicatorDetails.EMPTY));
        assertThat(result.impacts(), is(STAGNATING_INDEX_IMPACT));
        assertThat(result.diagnosisList().isEmpty(), is(true));
    }

    public void testMultiProject() {
        service = new DataStreamLifecycleHealthIndicatorService(TestProjectResolvers.allProjects());
        ProjectId projectId1 = randomProjectIdOrDefault();
        ProjectId projectId2 = randomUniqueProjectId();
        String index1 = DataStream.getDefaultBackingIndexName("foo", 1L);
        String index2 = DataStream.getDefaultBackingIndexName("boo", 1L);
        String index1DisplayName = projectId1 + ProjectIndexName.DELIMITER + index1;
        String index2DisplayName = projectId2 + ProjectIndexName.DELIMITER + index2;

        HealthIndicatorResult result = service.calculate(
            true,
            constructHealthInfo(
                new DataStreamLifecycleHealthInfo(
                    List.of(new DslErrorInfo(index1, 1L, 100, projectId1), new DslErrorInfo(index2, 3L, 100, projectId2)),
                    15
                )
            )
        );

        assertThat(result.status(), is(HealthStatus.YELLOW));
        assertThat(result.symptom(), is("2 backing indices have repeatedly encountered errors whilst trying to advance in its lifecycle"));
        assertThat(result.details(), is(not(HealthIndicatorDetails.EMPTY)));
        String detailsAsString = Strings.toString(result.details());
        assertThat(detailsAsString, containsString("\"total_backing_indices_in_error\":15"));
        assertThat(detailsAsString, containsString("\"stagnating_backing_indices_count\":2"));
        assertThat(
            detailsAsString,
            containsString(
                String.format(
                    Locale.ROOT,
                    "\"index_name\":\"%s\","
                        + "\"first_occurrence_timestamp\":1,\"retry_count\":100},{\"index_name\":\"%s\","
                        + "\"first_occurrence_timestamp\":3,\"retry_count\":100",
                    index1DisplayName,
                    index2DisplayName
                )
            )
        );
        assertThat(result.impacts(), is(STAGNATING_INDEX_IMPACT));
        Diagnosis diagnosis = result.diagnosisList().get(0);
        assertThat(diagnosis.definition(), is(STAGNATING_BACKING_INDICES_DIAGNOSIS_DEF));
        assertThat(diagnosis.affectedResources().get(0).getValues(), containsInAnyOrder(index1DisplayName, index2DisplayName));
    }

    private HealthInfo constructHealthInfo(DataStreamLifecycleHealthInfo dslHealthInfo) {
        return new HealthInfo(Map.of(), dslHealthInfo, Map.of(), FileSettingsHealthInfo.INDETERMINATE);
    }
}
