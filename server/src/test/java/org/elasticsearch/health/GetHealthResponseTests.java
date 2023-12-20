/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.health.GetHealthAction.Response;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.is;

public class GetHealthResponseTests extends ESTestCase {

    public void testToXContent() throws IOException {
        List<HealthIndicatorResult> indicatorResults = new ArrayList<>(2);
        indicatorResults.add(createRandomIndicatorResult());
        indicatorResults.add(createRandomIndicatorResult());
        Response response = new Response(ClusterName.DEFAULT, indicatorResults, true);

        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        response.toXContentChunked(EMPTY_PARAMS).forEachRemaining(xcontent -> {
            try {
                xcontent.toXContent(builder, EMPTY_PARAMS);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                fail(e.getMessage());
            }
        });
        Map<String, Object> xContentMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
        assertThat(xContentMap.get("status"), is(response.getStatus().xContentValue()));
        assertXContentContainsExpectedResult(xContentMap, indicatorResults.get(0).name(), indicatorResults.get(0));
        assertXContentContainsExpectedResult(xContentMap, indicatorResults.get(1).name(), indicatorResults.get(1));
    }

    @SuppressWarnings("unchecked")
    private void assertXContentContainsExpectedResult(
        Map<String, Object> xContentMap,
        String indicatorName,
        HealthIndicatorResult expectedIndicatorResult
    ) {
        Map<String, Object> allIndicators = (Map<String, Object>) xContentMap.get("indicators");
        Map<String, Object> indicator = (Map<String, Object>) allIndicators.get(indicatorName);
        List<Map<String, Object>> impacts = (List<Map<String, Object>>) indicator.get("impacts");
        List<HealthIndicatorImpact> fromXContentImpacts = new ArrayList<>(impacts.size());
        for (Map<String, Object> impact : impacts) {
            List<ImpactArea> impactAreasList = new ArrayList<>();
            List<String> fromXContentImpactAreas = (List<String>) impact.get("impact_areas");
            for (String impactArea : fromXContentImpactAreas) {
                impactAreasList.add(ImpactArea.valueOf(impactArea.toUpperCase(Locale.ROOT)));
            }
            String xcontentId = (String) impact.get("id");
            fromXContentImpacts.add(
                new HealthIndicatorImpact(
                    indicatorName,
                    // TODO add some methods to handle the id and xcontent id transitions
                    xcontentId.substring(xcontentId.lastIndexOf(":") + 1),
                    (Integer) impact.get("severity"),
                    (String) impact.get("description"),
                    impactAreasList
                )
            );
        }

        assertThat(fromXContentImpacts, is(expectedIndicatorResult.impacts()));
        List<Map<String, Object>> diagnosisList = (List<Map<String, Object>>) indicator.get("diagnosis");

        List<Diagnosis> fromXContentDiagnosis = new ArrayList<>(diagnosisList.size());
        for (Map<String, Object> diagnosisMap : diagnosisList) {
            String xcontentId = (String) diagnosisMap.get("id");
            Map<String, Object> affectedResources = (Map<String, Object>) diagnosisMap.get("affected_resources");
            Diagnosis.Resource affectedIndices = new Diagnosis.Resource(
                Diagnosis.Resource.Type.INDEX,
                (List<String>) affectedResources.get("indices")
            );

            fromXContentDiagnosis.add(
                new Diagnosis(
                    new Diagnosis.Definition(
                        indicatorName,
                        xcontentId.substring(xcontentId.lastIndexOf(":") + 1),
                        (String) diagnosisMap.get("cause"),
                        (String) diagnosisMap.get("action"),
                        (String) diagnosisMap.get("help_url")
                    ),
                    List.of(affectedIndices)
                )
            );
        }

        assertThat(fromXContentDiagnosis, is(expectedIndicatorResult.diagnosisList()));

        HealthIndicatorResult fromXContentIndicatorResult = new HealthIndicatorResult(
            indicatorName,
            HealthStatus.valueOf(((String) indicator.get("status")).toUpperCase(Locale.ROOT)),
            (String) indicator.get("symptom"),
            new SimpleHealthIndicatorDetails((Map<String, Object>) indicator.get("details")),
            fromXContentImpacts,
            fromXContentDiagnosis
        );

        assertThat(fromXContentIndicatorResult, is(expectedIndicatorResult));
    }

    private static HealthIndicatorResult createRandomIndicatorResult() {
        String name = randomAlphaOfLength(10);
        HealthStatus status = randomFrom(HealthStatus.RED, HealthStatus.YELLOW, HealthStatus.GREEN);
        String symptom = randomAlphaOfLength(20);
        Map<String, Object> detailsMap = new HashMap<>();
        detailsMap.put(randomAlphaOfLengthBetween(5, 50), randomAlphaOfLengthBetween(5, 50));
        HealthIndicatorDetails details = new SimpleHealthIndicatorDetails(detailsMap);
        List<HealthIndicatorImpact> impacts = new ArrayList<>();
        String impact1Id = randomAlphaOfLength(30);
        int impact1Severity = randomIntBetween(1, 5);
        String impact1Description = randomAlphaOfLength(30);
        ImpactArea firstImpactArea = randomFrom(ImpactArea.values());
        impacts.add(new HealthIndicatorImpact(name, impact1Id, impact1Severity, impact1Description, List.of(firstImpactArea)));
        String impact2Id = randomAlphaOfLength(30);
        int impact2Severity = randomIntBetween(1, 5);
        String impact2Description = randomAlphaOfLength(30);
        ImpactArea secondImpactArea = randomFrom(ImpactArea.values());
        impacts.add(new HealthIndicatorImpact(name, impact2Id, impact2Severity, impact2Description, List.of(secondImpactArea)));
        List<Diagnosis> diagnosisList = new ArrayList<>();
        Diagnosis.Resource resource1 = new Diagnosis.Resource(Diagnosis.Resource.Type.INDEX, List.of(randomAlphaOfLength(10)));
        Diagnosis diagnosis1 = new Diagnosis(
            new Diagnosis.Definition(
                name,
                randomAlphaOfLength(30),
                randomAlphaOfLength(50),
                randomAlphaOfLength(50),
                randomAlphaOfLength(30)
            ),
            List.of(resource1)
        );
        diagnosisList.add(diagnosis1);
        Diagnosis.Resource resource2 = new Diagnosis.Resource(Diagnosis.Resource.Type.INDEX, List.of(randomAlphaOfLength(10)));
        Diagnosis diagnosis2 = new Diagnosis(
            new Diagnosis.Definition(
                name,
                randomAlphaOfLength(30),
                randomAlphaOfLength(50),
                randomAlphaOfLength(50),
                randomAlphaOfLength(30)
            ),
            List.of(resource2)
        );
        diagnosisList.add(diagnosis2);
        return new HealthIndicatorResult(name, status, symptom, details, impacts, diagnosisList);
    }

}
