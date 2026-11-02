/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.telemetry.KibanaExecutionContextClassifier.Feature;
import org.elasticsearch.xpack.esql.telemetry.KibanaExecutionContextClassifier.Interaction;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.xpack.esql.telemetry.KibanaExecutionContextClassifier.classify;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * The contexts below are the wire form Kibana sends in {@code X-Opaque-Id}: {@code <requestId>;kibana:<node>[;<child node>...]},
 * each node {@code type:name:id} with every part URI-encoded (Kibana's {@code execution_context_container.ts}).
 */
public class KibanaExecutionContextClassifierTests extends ESTestCase {

    private static final List<String> KIBANA_ORIGINS = List.of("kibana", "Kibana", "KIBANA", " kibana ", "kibana-reporting");

    public void testDiscover() {
        assertKibana("application:discover:new", Feature.DISCOVER);
        assertKibana("application:discover:5a0e4b1c-7e2f-4d1a-9b3c-2f1e0d9c8b7a", Feature.DISCOVER);
    }

    public void testDashboardViewedUnderApplicationRoot() {
        assertKibana("application:dashboards:7adfa750-4c81-11e8-b3d7-01146121b73d;lens:lnsXY:panel-1", Feature.DASHBOARDS);
    }

    public void testDashboardPanelUnderDashboardRoot() {
        assertKibana("dashboard:dashboards:7adfa750-4c81-11e8-b3d7-01146121b73d;lens:lnsXY:panel-1", Feature.DASHBOARDS);
        assertKibana("dashboard:dashboards:7adfa750;discover_session:discover:abc", Feature.DASHBOARDS);
    }

    public void testDashboardListingWithoutId() {
        assertKibana("application:dashboards:", Feature.DASHBOARDS);
        assertKibana("application:dashboards:;lens:lnsMetric:panel-2", Feature.DASHBOARDS);
    }

    public void testLensEditor() {
        assertKibana("application:lens:new;lens::", Feature.LENS);
        assertKibana("application:lens:a1b2c3", Feature.LENS);
    }

    public void testMaps() {
        assertKibana("application:maps:new", Feature.MAPS);
    }

    public void testCanvas() {
        assertKibana("application:canvas:", Feature.CANVAS);
    }

    public void testDevTools() {
        assertKibana("application:dev_tools:", Feature.DEV_TOOLS);
    }

    public void testStackEsQueryRule() {
        assertKibana(ruleContext(".es-query"), Feature.STACK_RULE);
    }

    public void testSecurityEsqlRule() {
        assertKibana(ruleContext("siem.esqlRule"), Feature.SECURITY_RULE);
    }

    public void testSecurityRuleTypes() {
        for (String ruleType : List.of("siem.queryRule", "siem.eqlRule", "siem.newTermsRule", "siem.thresholdRule", "attack-discovery")) {
            assertKibana(ruleContext(ruleType), Feature.SECURITY_RULE);
        }
    }

    public void testObservabilityRuleTypes() {
        for (String ruleType : List.of(
            "observability.rules.custom_threshold",
            "slo.rules.burnRate",
            "logs.alert.document.count",
            "metrics.alert.threshold",
            "metrics.alert.inventory.threshold",
            "apm.error_rate",
            "xpack.uptime.alerts.monitorStatus",
            "xpack.synthetics.alerts.monitorStatus",
            "datasetQuality.degradedDocs"
        )) {
            assertKibana(ruleContext(ruleType), Feature.OBSERVABILITY_RULE);
        }
    }

    public void testOtherRuleTypesAreStack() {
        for (String ruleType : List.of(
            ".index-threshold",
            ".geo-containment",
            "xpack.ml.anomaly_detection_alert",
            "transform_health",
            "monitoring_alert_cpu_usage",
            "some.future.rule"
        )) {
            assertKibana(ruleContext(ruleType), Feature.STACK_RULE);
        }
    }

    public void testRuleWithoutAlertChildUsesTaskType() {
        assertKibana("task%20manager:run%20alerting%3Asiem.esqlRule:task-1", Feature.SECURITY_RULE);
        assertKibana("task%20manager:run%20alerting%3A.es-query:task-1", Feature.STACK_RULE);
    }

    public void testRuleTaskWithEmptyRuleType() {
        assertKibana("task%20manager:run%20alerting%3A:task-1", Feature.OTHER_TASK_MANAGER);
    }

    public void testBackfillUsesAlertChild() {
        assertKibana("task%20manager:run%20ad_hoc_run-backfill:task-1;alert:execute%20siem.queryRule:rule-1", Feature.SECURITY_RULE);
        assertKibana("task%20manager:run%20ad_hoc_run-backfill:task-1;alert:execute%20.es-query:rule-1", Feature.STACK_RULE);
    }

    public void testBackfillWithoutAlertChild() {
        assertKibana("task%20manager:run%20ad_hoc_run-backfill:task-1", Feature.OTHER_TASK_MANAGER);
        assertKibana("task%20manager:run%20ad_hoc_run-backfill:task-1;foo:bar:baz", Feature.OTHER_TASK_MANAGER);
    }

    public void testAlertingV2Dispatcher() {
        assertKibana("task%20manager:run%20alerting_v2%3Adispatcher:alerting_v2%3Adispatcher%3A1.0.0", Feature.ALERTING_V2);
    }

    public void testAlertingV2RuleExecutor() {
        assertKibana("task%20manager:run%20alerting_v2%3Arule_executor:task-1", Feature.ALERTING_V2);
    }

    public void testEntityStore() {
        assertKibana(
            "task%20manager:run%20entity_store%3Av2%3Aextract_entity_task%3Ahost:task-1;"
                + "security_solution:entity_analytics%3Aentity_store_extract_task:task-1",
            Feature.ENTITY_STORE
        );
        for (String taskType : List.of(
            "entity_store%3Av2%3Aextract_entity_non_priority_task%3Auser",
            "entity_store%3Av2%3Aentity_maintainer_task%3Arisk",
            "entity_store%3Av2%3Ahistory_snapshot_task",
            "entity_store%3Av2%3Aresilience_task",
            "entity_store%3Av2%3Astatus_report_task",
            "entity_store%3Av2%3Alegacy_security_assets_migration"
        )) {
            assertKibana("task%20manager:run%20" + taskType + ":task-1", Feature.ENTITY_STORE);
        }
    }

    public void testReporting() {
        assertKibana("task%20manager:run%20report%3Aexecute:task-1", Feature.REPORTING);
        assertKibana("task%20manager:run%20report%3Aexecute-scheduled:task-1", Feature.REPORTING);
    }

    public void testOtherTaskManager() {
        assertKibana("task%20manager:run%20fleet%3Acheck-deleted-files-task:task-1", Feature.OTHER_TASK_MANAGER);
        assertKibana("task%20manager:taskStore:", Feature.OTHER_TASK_MANAGER);
        assertKibana("task%20manager:run%20:task-1", Feature.OTHER_TASK_MANAGER);
    }

    /**
     * The examples recorded in esql-planning#2483, which were written down decoded. Decoded and encoded forms must agree.
     */
    public void testDecodedExamplesFromIssue() {
        assertKibana("application:discover:abc", Feature.DISCOVER);
        assertKibana("dashboard:dashboards:7adfa750;lens:lnsXY:panel-1", Feature.DASHBOARDS);
        assertKibana("task manager:run alerting:.es-query:task-1;alert:execute .es-query:rule-1", Feature.STACK_RULE);
        assertKibana("task manager:run alerting:siem.esqlRule:task-1;alert:execute siem.esqlRule:rule-1", Feature.SECURITY_RULE);
        assertKibana("task manager:run alerting_v2:dispatcher:alerting_v2:dispatcher:1.0.0", Feature.ALERTING_V2);
        assertKibana("task manager:run report:execute:task-1", Feature.REPORTING);
    }

    public void testContextWithoutRequestId() {
        assertThat(classify(randomKibanaOrigin(), "kibana:application:discover:abc"), is(Feature.DISCOVER));
    }

    public void testAlertRootWithoutTaskManager() {
        assertKibana("alert:execute%20siem.esqlRule:rule-1", Feature.SECURITY_RULE);
        assertKibana("alert:execute%20:rule-1", Feature.OTHER_KIBANA);
    }

    public void testRepeatedRootNode() {
        assertKibana("application:discover:abc;application:discover:abc", Feature.DISCOVER);
    }

    public void testRequestIdContainingContextMarkerIsIgnored() {
        String opaqueId = "spoof;kibana:application:discover:x;kibana:task%20manager:run%20report%3Aexecute:task-1";
        assertThat(classify(randomKibanaOrigin(), opaqueId), is(Feature.REPORTING));
    }

    public void testUnknownApplication() {
        assertKibana("application:securitySolutionUI:", Feature.OTHER_KIBANA);
        assertKibana("dashboard:securitySolutionUI:abc", Feature.OTHER_KIBANA);
        assertKibana("application::", Feature.OTHER_KIBANA);
        assertKibana("application:Discover:abc", Feature.OTHER_KIBANA);
    }

    public void testUnknownRootType() {
        assertKibana("foo:bar:baz", Feature.OTHER_KIBANA);
        assertKibana("security_solution:entity_analytics%3Aentity_store_extract_task:task-1", Feature.OTHER_KIBANA);
    }

    public void testKibanaWithoutExecutionContext() {
        assertThat(classify(randomKibanaOrigin(), UUIDs.randomBase64UUID(random())), is(Feature.OTHER_KIBANA));
        assertThat(classify(randomKibanaOrigin(), "unknownId"), is(Feature.OTHER_KIBANA));
    }

    public void testKibanaWithNullOpaqueId() {
        assertThat(classify(randomKibanaOrigin(), null), is(Feature.OTHER_KIBANA));
    }

    public void testKibanaWithEmptyOpaqueId() {
        assertThat(classify(randomKibanaOrigin(), ""), is(Feature.OTHER_KIBANA));
    }

    public void testKibanaOrigins() {
        for (String origin : KIBANA_ORIGINS) {
            assertThat(origin, classify(origin, header("application:discover:abc")), is(Feature.DISCOVER));
        }
    }

    public void testNonKibanaOriginIsNotClassified() {
        for (String origin : Arrays.asList(null, "", "  ", "logstash", "kibanana", "beats", "fleet")) {
            assertThat(origin, classify(origin, header("application:discover:abc")), nullValue());
        }
    }

    public void testTruncatedEscapeAtEnd() {
        assertKibana("task%20manager:run%20report%3", Feature.OTHER_TASK_MANAGER);
        assertKibana("task%20manager:run%20report%", Feature.OTHER_TASK_MANAGER);
        assertKibana("task%2", Feature.OTHER_KIBANA);
    }

    public void testEscapeEndingTheNode() {
        assertKibana("task%20manager:run%20alerting_v2%3A", Feature.ALERTING_V2);
        assertKibana("task%20manager:run%20alerting_v2%3A;lens::", Feature.ALERTING_V2);
    }

    public void testInvalidEscapes() {
        assertKibana("application:discover%ZZ:abc", Feature.OTHER_KIBANA);
        assertKibana("application:discover:%ZZ%", Feature.DISCOVER);
        assertKibana("%", Feature.OTHER_KIBANA);
        assertKibana("%%%%", Feature.OTHER_KIBANA);
    }

    public void testNonAsciiEscapes() {
        assertKibana("application:discover:%E2%98%BA%E6%BC%A2", Feature.DISCOVER);
        assertKibana("test%E2%98%BAtype:test%E6%BC%A2name:42", Feature.OTHER_KIBANA);
        assertKibana("application:%E2%98%BA:42", Feature.OTHER_KIBANA);
    }

    /**
     * Kibana cuts the whole context to 1024 characters, which can end a node mid-way or drop the children.
     */
    public void testContextCutAtKibanaLimit() {
        String context = ruleContext("siem.esqlRule") + ";" + "lens:lnsXY:".repeat(200);
        assertKibana(context.substring(0, 1024), Feature.SECURITY_RULE);
        String longRoot = "application:discover:" + "a".repeat(2000);
        assertKibana(longRoot.substring(0, 1024), Feature.DISCOVER);
    }

    public void testGarbage() {
        for (String opaqueId : List.of(
            ";",
            ";;;",
            ";kibana:",
            ";kibana:;;;",
            "kibana:",
            ":::",
            ";kibana::::",
            "   ",
            "\t\n\r",
            "\u0000\u0001;kibana:\u0000",
            "\uD800;kibana:application:\uDC00",
            ";kibana:\uD800",
            "kibana:;kibana:;kibana:"
        )) {
            assertThat(opaqueId, classify(randomKibanaOrigin(), opaqueId), is(Feature.OTHER_KIBANA));
        }
    }

    public void testVeryLongInput() {
        String junk = randomAlphaOfLength(1 << 20);
        assertThat(classify(randomKibanaOrigin(), junk), is(Feature.OTHER_KIBANA));
        assertThat(classify(randomKibanaOrigin(), header("application:discover:abc;" + junk)), is(Feature.DISCOVER));
        assertThat(classify(randomKibanaOrigin(), header("application:discover:" + junk)), is(Feature.DISCOVER));
        assertThat(classify(randomKibanaOrigin(), header("task%20manager:run%20" + junk)), is(Feature.OTHER_TASK_MANAGER));
        assertThat(classify(randomKibanaOrigin(), ";kibana:".repeat(1 << 16)), is(Feature.OTHER_KIBANA));
        assertThat(classify(randomKibanaOrigin(), "%".repeat(1 << 20)), is(Feature.OTHER_KIBANA));
    }

    public void testFuzzNeverThrows() {
        String alphabet = ";:%kibanaplicto rugdshvx_.-0123456789ABCDEFabcdef";
        for (int i = 0; i < 1000; i++) {
            String opaqueId = switch (between(0, 2)) {
                case 0 -> randomUnicodeOfLengthBetween(0, 200);
                case 1 -> randomString(alphabet, between(0, 200));
                case 2 -> header(randomString(alphabet, between(0, 200)));
                default -> throw new AssertionError();
            };
            assertThat(opaqueId, classify(randomKibanaOrigin(), opaqueId), notNullValue());
            assertThat(opaqueId, classify("logstash", opaqueId), nullValue());
        }
    }

    public void testFeatureLabels() {
        assertThat(
            Arrays.stream(Feature.values()).map(Feature::label).toList(),
            contains(
                "discover",
                "dashboards",
                "lens",
                "maps",
                "canvas",
                "dev_tools",
                "stack_rule",
                "security_rule",
                "observability_rule",
                "alerting_v2",
                "entity_store",
                "reporting",
                "other_task_manager",
                "other_kibana"
            )
        );
    }

    public void testInteractionLabels() {
        assertThat(Arrays.stream(Interaction.values()).map(Interaction::label).toList(), contains("person", "background", "unknown"));
    }

    public void testFeatureInteraction() {
        Map<Feature, Interaction> expected = Map.ofEntries(
            Map.entry(Feature.DISCOVER, Interaction.PERSON),
            Map.entry(Feature.DASHBOARDS, Interaction.PERSON),
            Map.entry(Feature.LENS, Interaction.PERSON),
            Map.entry(Feature.MAPS, Interaction.PERSON),
            Map.entry(Feature.CANVAS, Interaction.PERSON),
            Map.entry(Feature.DEV_TOOLS, Interaction.PERSON),
            Map.entry(Feature.STACK_RULE, Interaction.BACKGROUND),
            Map.entry(Feature.SECURITY_RULE, Interaction.BACKGROUND),
            Map.entry(Feature.OBSERVABILITY_RULE, Interaction.BACKGROUND),
            Map.entry(Feature.ALERTING_V2, Interaction.BACKGROUND),
            Map.entry(Feature.ENTITY_STORE, Interaction.BACKGROUND),
            Map.entry(Feature.REPORTING, Interaction.BACKGROUND),
            Map.entry(Feature.OTHER_TASK_MANAGER, Interaction.BACKGROUND),
            Map.entry(Feature.OTHER_KIBANA, Interaction.UNKNOWN)
        );
        Map<Feature, Interaction> actual = Arrays.stream(Feature.values()).collect(toMap(f -> f, Feature::interaction));
        assertThat(actual, equalTo(expected));
    }

    private static void assertKibana(String context, Feature expected) {
        String opaqueId = header(context);
        assertThat(opaqueId, classify(randomKibanaOrigin(), opaqueId), is(expected));
    }

    private static String header(String context) {
        String requestId = randomFrom(UUIDs.randomBase64UUID(random()), "unknownId", "6b6a3bb3-1c5c-4f43-a3e2-9b2b3f0e6d7a");
        return requestId + ";kibana:" + context;
    }

    private static String ruleContext(String ruleType) {
        return "task%20manager:run%20alerting%3A" + ruleType + ":task-1;alert:execute%20" + ruleType + ":rule-1";
    }

    private static String randomKibanaOrigin() {
        return randomFrom(KIBANA_ORIGINS);
    }

    private static String randomString(String alphabet, int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(alphabet.charAt(between(0, alphabet.length() - 1)));
        }
        return sb.toString();
    }
}
