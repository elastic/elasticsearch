/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ilm.LifecycleSettings.LIFECYCLE_POLL_INTERVAL_SETTING;
import static org.elasticsearch.xpack.deprecation.DeprecationChecks.CLUSTER_SETTINGS_CHECKS;
import static org.elasticsearch.xpack.deprecation.IndexDeprecationChecksTests.addRandomFields;

public class ClusterDeprecationChecksTests extends ESTestCase {

    public void testUserAgentEcsCheck() {
        PutPipelineRequest ecsFalseRequest = new PutPipelineRequest("ecs_false",
            new BytesArray("{\n" +
                "  \"description\" : \"This has ecs set to false\",\n" +
                "  \"processors\" : [\n" +
                "    {\n" +
                "      \"user_agent\" : {\n" +
                "        \"field\" : \"agent\",\n" +
                "        \"ecs\" : false\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}"), XContentType.JSON);
        PutPipelineRequest ecsNullRequest = new PutPipelineRequest("ecs_null",
            new BytesArray("{\n" +
                "  \"description\" : \"This has ecs set to false\",\n" +
                "  \"processors\" : [\n" +
                "    {\n" +
                "      \"user_agent\" : {\n" +
                "        \"field\" : \"agent\"\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}"), XContentType.JSON);
        PutPipelineRequest ecsTrueRequest = new PutPipelineRequest("ecs_true",
            new BytesArray("{\n" +
                "  \"description\" : \"This has ecs set to false\",\n" +
                "  \"processors\" : [\n" +
                "    {\n" +
                "      \"user_agent\" : {\n" +
                "        \"field\" : \"agent\",\n" +
                "        \"ecs\" : true\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}"), XContentType.JSON);

        ClusterState state = ClusterState.builder(new ClusterName("test")).build();
        state = IngestService.innerPut(ecsTrueRequest, state);
        state = IngestService.innerPut(ecsFalseRequest, state);
        state = IngestService.innerPut(ecsNullRequest, state);

        final ClusterState finalState = state;
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(finalState));

        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.WARNING,
            "User-Agent ingest plugin will always use ECS-formatted output",
            "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-8.0.html" +
                "#ingest-user-agent-ecs-always",
            "Ingest pipelines [ecs_false, ecs_true] uses the [ecs] option which needs to be removed to work in 8.0");
        assertEquals(singletonList(expected), issues);
    }

    public void testTemplateWithTooManyFields() throws IOException {
        String tooManyFieldsTemplate = randomAlphaOfLength(5);
        String tooManyFieldsWithDefaultFieldsTemplate = randomAlphaOfLength(6);
        String goodTemplateName = randomAlphaOfLength(7);

        // A template with too many fields
        int tooHighFieldCount = randomIntBetween(1025, 10_000); // 10_000 is arbitrary
        XContentBuilder badMappingBuilder = jsonBuilder();
        badMappingBuilder.startObject();
        {
            badMappingBuilder.startObject("_doc");
            {
                badMappingBuilder.startObject("properties");
                {
                    addRandomFields(tooHighFieldCount, badMappingBuilder);
                }
                badMappingBuilder.endObject();
            }
            badMappingBuilder.endObject();
        }
        badMappingBuilder.endObject();

        // A template with an OK number of fields
        int okFieldCount = randomIntBetween(1, 1024);
        XContentBuilder goodMappingBuilder = jsonBuilder();
        goodMappingBuilder.startObject();
        {
            goodMappingBuilder.startObject("_doc");
            {
                goodMappingBuilder.startObject("properties");
                {
                    addRandomFields(okFieldCount, goodMappingBuilder);
                }
                goodMappingBuilder.endObject();
            }
            goodMappingBuilder.endObject();
        }
        goodMappingBuilder.endObject();

        final ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(5)))
            .metaData(MetaData.builder()
                .put(IndexTemplateMetaData.builder(tooManyFieldsTemplate)
                    .patterns(Collections.singletonList(randomAlphaOfLength(5)))
                    .putMapping("_doc", Strings.toString(badMappingBuilder))
                    .build())
                .put(IndexTemplateMetaData.builder(tooManyFieldsWithDefaultFieldsTemplate)
                    .patterns(Collections.singletonList(randomAlphaOfLength(5)))
                    .putMapping("_doc", Strings.toString(badMappingBuilder))
                    .settings(Settings.builder()
                        .put(IndexSettings.DEFAULT_FIELD_SETTING.getKey(),
                            Collections.singletonList(randomAlphaOfLength(5)).toString()))
                    .build())
                .put(IndexTemplateMetaData.builder(goodTemplateName)
                    .patterns(Collections.singletonList(randomAlphaOfLength(5)))
                    .putMapping("_doc", Strings.toString(goodMappingBuilder))
                    .build())
                .build())
            .build();

        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(state));

        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.WARNING,
            "Fields in index template exceed automatic field expansion limit",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                "#_limiting_the_number_of_auto_expanded_fields",
            "Index templates " + Collections.singletonList(tooManyFieldsTemplate) + " have a number of fields which exceeds the " +
                "automatic field expansion limit of [1024] and does not have [" + IndexSettings.DEFAULT_FIELD_SETTING.getKey() + "] set, " +
                "which may cause queries which use automatic field expansion, such as query_string, simple_query_string, and multi_match " +
                "to fail if fields are not explicitly specified in the query.");
        assertEquals(singletonList(expected), issues);
    }

    public void testTemplatesWithFieldNamesDisabled() throws IOException {
        XContentBuilder goodMappingBuilder = jsonBuilder();
        goodMappingBuilder.startObject();
        {
            goodMappingBuilder.startObject("_doc");
            {
                goodMappingBuilder.startObject("properties");
                {
                    addRandomFields(10, goodMappingBuilder);
                }
                goodMappingBuilder.endObject();
            }
            goodMappingBuilder.endObject();
        }
        goodMappingBuilder.endObject();
        assertFieldNamesEnabledTemplate(goodMappingBuilder, false);

        XContentBuilder badMappingBuilder = jsonBuilder();
        badMappingBuilder.startObject();
        {
            // we currently always store a type level internally
            badMappingBuilder.startObject("_doc");
            {
                badMappingBuilder.startObject(FieldNamesFieldMapper.NAME);
                {
                    badMappingBuilder.field("enabled", randomBoolean());
                }
                badMappingBuilder.endObject();
            }
            badMappingBuilder.endObject();
        }
        badMappingBuilder.endObject();
        assertFieldNamesEnabledTemplate(badMappingBuilder, true);

        // however, there was a bug where mappings could be stored without a type (#45120)
        // so we also should try to check these cases

        XContentBuilder badMappingWithoutTypeBuilder = jsonBuilder();
        badMappingWithoutTypeBuilder.startObject();
        {
            badMappingWithoutTypeBuilder.startObject(FieldNamesFieldMapper.NAME);
            {
                badMappingWithoutTypeBuilder.field("enabled", randomBoolean());
            }
            badMappingWithoutTypeBuilder.endObject();
        }
        badMappingWithoutTypeBuilder.endObject();
        assertFieldNamesEnabledTemplate(badMappingWithoutTypeBuilder, true);
    }

    private void assertFieldNamesEnabledTemplate(XContentBuilder templateBuilder, boolean expectIssue) throws IOException {
        String badTemplateName = randomAlphaOfLength(5);
        final ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(5)))
            .metaData(MetaData.builder()
                .put(IndexTemplateMetaData.builder(badTemplateName)
                    .patterns(Collections.singletonList(randomAlphaOfLength(5)))
                    .putMapping("_doc", Strings.toString(templateBuilder))
                    .build())
                .build())
            .build();

        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(state));
        if (expectIssue) {
            assertEquals(1, issues.size());
            DeprecationIssue issue = issues.get(0);
            assertEquals(DeprecationIssue.Level.WARNING, issue.getLevel());
            assertEquals("https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-8.0.html#fieldnames-enabling"
                    , issue.getUrl());
            assertEquals("Index templates contain _field_names settings.", issue.getMessage());
            assertEquals("Index templates [" + badTemplateName + "] "
                    + "use the deprecated `enable` setting for the `" + FieldNamesFieldMapper.NAME +
                    "` field. Using this setting in new index mappings will throw an error in the next major version and " +
                    "needs to be removed from existing mappings and templates.", issue.getDetails());
        } else {
            assertTrue(issues.isEmpty());
        }
    }

    public void testPollIntervalTooLow() {
        {
            final String tooLowInterval = randomTimeValue(1, 999, "ms", "micros", "nanos");
            MetaData badMetaDtata = MetaData.builder()
                .persistentSettings(Settings.builder()
                    .put(LIFECYCLE_POLL_INTERVAL_SETTING.getKey(), tooLowInterval)
                    .build())
                .build();
            ClusterState badState = ClusterState.builder(new ClusterName("test"))
                .metaData(badMetaDtata)
                .build();

            DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Index Lifecycle Management poll interval is set too low",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-8.0.html" +
                    "#ilm-poll-interval-limit",
                "The Index Lifecycle Management poll interval setting [" + LIFECYCLE_POLL_INTERVAL_SETTING.getKey() + "] is " +
                    "currently set to [" + tooLowInterval + "], but must be 1s or greater");
            List<DeprecationIssue> issues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(badState));
            assertEquals(singletonList(expected), issues);
        }

        // Test that other values are ok
        {
            final String okInterval = randomTimeValue(1, 9999, "d", "h", "s");
            MetaData okMetaData = MetaData.builder()
                .persistentSettings(Settings.builder()
                    .put(LIFECYCLE_POLL_INTERVAL_SETTING.getKey(), okInterval)
                    .build())
                .build();
            ClusterState okState = ClusterState.builder(new ClusterName("test"))
                .metaData(okMetaData)
                .build();
            List<DeprecationIssue> noIssues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(okState));
            assertThat(noIssues, Matchers.hasSize(0));
        }
    }
}
