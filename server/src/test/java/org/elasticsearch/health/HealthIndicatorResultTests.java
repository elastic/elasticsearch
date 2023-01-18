/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.health.HealthService.HEALTH_API_ID_PREFIX;

public class HealthIndicatorResultTests extends ESTestCase {
    public void testToXContent() throws Exception {
        String name = randomAlphaOfLength(10);
        HealthStatus status = randomFrom(HealthStatus.RED, HealthStatus.YELLOW, HealthStatus.GREEN);
        String symptom = randomAlphaOfLength(20);
        Map<String, Object> detailsMap = new HashMap<>();
        detailsMap.put("key", "value");
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
        HealthIndicatorResult result = new HealthIndicatorResult(name, status, symptom, details, impacts, diagnosisList);
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();

        result.toXContentChunked(ToXContent.EMPTY_PARAMS).forEachRemaining(xcontent -> {
            try {
                xcontent.toXContent(builder, ToXContent.EMPTY_PARAMS);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                fail(e.getMessage());
            }
        });
        Map<String, Object> xContentMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
        assertEquals(status.xContentValue(), xContentMap.get("status"));
        assertEquals(symptom, xContentMap.get("symptom"));
        assertEquals(detailsMap, xContentMap.get("details"));
        List<Map<String, Object>> expectedImpacts = new ArrayList<>();
        Map<String, Object> expectedImpact1 = new HashMap<>();
        expectedImpact1.put("id", HEALTH_API_ID_PREFIX + name + ":impact:" + impact1Id);
        expectedImpact1.put("severity", impact1Severity);
        expectedImpact1.put("description", impact1Description);
        expectedImpact1.put("impact_areas", List.of(firstImpactArea.displayValue()));
        Map<String, Object> expectedImpact2 = new HashMap<>();
        expectedImpact2.put("id", HEALTH_API_ID_PREFIX + name + ":impact:" + impact2Id);
        expectedImpact2.put("severity", impact2Severity);
        expectedImpact2.put("description", impact2Description);
        expectedImpact2.put("impact_areas", List.of(secondImpactArea.displayValue()));
        expectedImpacts.add(expectedImpact1);
        expectedImpacts.add(expectedImpact2);
        assertEquals(expectedImpacts, xContentMap.get("impacts"));
        List<Map<String, Object>> expectedDiagnosis = new ArrayList<>();
        {
            Map<String, Object> expectedDiagnosis1 = new HashMap<>();
            expectedDiagnosis1.put("id", HEALTH_API_ID_PREFIX + name + ":diagnosis:" + diagnosis1.definition().id());
            expectedDiagnosis1.put("cause", diagnosis1.definition().cause());
            expectedDiagnosis1.put("action", diagnosis1.definition().action());
            expectedDiagnosis1.put("help_url", diagnosis1.definition().helpURL());

            if (diagnosis1.affectedResources() != null) {
                XContentBuilder diagnosisXContent = XContentFactory.jsonBuilder().prettyPrint();
                diagnosis1.toXContentChunked(ToXContent.EMPTY_PARAMS).forEachRemaining(xcontent -> {
                    try {
                        xcontent.toXContent(diagnosisXContent, ToXContent.EMPTY_PARAMS);
                    } catch (IOException e) {
                        logger.error(e.getMessage(), e);
                        fail(e.getMessage());
                    }
                });
                expectedDiagnosis1.put(
                    "affected_resources",
                    XContentHelper.convertToMap(BytesReference.bytes(diagnosisXContent), false, builder.contentType())
                        .v2()
                        .get("affected_resources")
                );
            }
            expectedDiagnosis.add(expectedDiagnosis1);
        }
        {
            Map<String, Object> expectedDiagnosis2 = new HashMap<>();
            expectedDiagnosis2.put("id", HEALTH_API_ID_PREFIX + name + ":diagnosis:" + diagnosis2.definition().id());
            expectedDiagnosis2.put("cause", diagnosis2.definition().cause());
            expectedDiagnosis2.put("action", diagnosis2.definition().action());
            expectedDiagnosis2.put("help_url", diagnosis2.definition().helpURL());
            if (diagnosis2.affectedResources() != null) {
                XContentBuilder diagnosisXContent = XContentFactory.jsonBuilder().prettyPrint();
                diagnosis2.toXContentChunked(ToXContent.EMPTY_PARAMS).forEachRemaining(xcontent -> {
                    try {
                        xcontent.toXContent(diagnosisXContent, ToXContent.EMPTY_PARAMS);
                    } catch (IOException e) {
                        logger.error(e.getMessage(), e);
                        fail(e.getMessage());
                    }
                });
                expectedDiagnosis2.put(
                    "affected_resources",
                    XContentHelper.convertToMap(BytesReference.bytes(diagnosisXContent), false, builder.contentType())
                        .v2()
                        .get("affected_resources")
                );
            }
            expectedDiagnosis.add(expectedDiagnosis2);
        }
        assertEquals(expectedDiagnosis, xContentMap.get("diagnosis"));
    }

    public void testChunkCount() {
        String name = randomAlphaOfLength(10);
        HealthStatus status = randomFrom(HealthStatus.RED, HealthStatus.YELLOW, HealthStatus.GREEN);
        String symptom = randomAlphaOfLength(20);
        Map<String, Object> detailsMap = new HashMap<>();
        detailsMap.put("key", "value");
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
        HealthIndicatorResult result = new HealthIndicatorResult(name, status, symptom, details, impacts, diagnosisList);

        // -> each Diagnosis yields 5 chunks => 10 chunks from both diagnosis
        // -> HealthIndicatorResult surrounds the diagnosis list by 2 chunks
        AbstractChunkedSerializingTestCase.assertChunkCount(result, ignored -> 12);
    }
}
