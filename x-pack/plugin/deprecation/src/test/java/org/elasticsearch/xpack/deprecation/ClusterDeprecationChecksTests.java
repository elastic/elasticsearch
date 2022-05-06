/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.Version;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.Phase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.deprecation.DeprecationChecks.CLUSTER_SETTINGS_CHECKS;
import static org.elasticsearch.xpack.deprecation.IndexDeprecationChecksTests.addRandomFields;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;

public class ClusterDeprecationChecksTests extends ESTestCase {

    public void testUserAgentEcsCheck() {
        PutPipelineRequest ecsFalseRequest = new PutPipelineRequest(
            "ecs_false",
            new BytesArray(
                "{\n"
                    + "  \"description\" : \"This has ecs set to false\",\n"
                    + "  \"processors\" : [\n"
                    + "    {\n"
                    + "      \"user_agent\" : {\n"
                    + "        \"field\" : \"agent\",\n"
                    + "        \"ecs\" : false\n"
                    + "      }\n"
                    + "    }\n"
                    + "  ]\n"
                    + "}"
            ),
            XContentType.JSON
        );
        PutPipelineRequest ecsNullRequest = new PutPipelineRequest(
            "ecs_null",
            new BytesArray(
                "{\n"
                    + "  \"description\" : \"This has ecs set to false\",\n"
                    + "  \"processors\" : [\n"
                    + "    {\n"
                    + "      \"user_agent\" : {\n"
                    + "        \"field\" : \"agent\"\n"
                    + "      }\n"
                    + "    }\n"
                    + "  ]\n"
                    + "}"
            ),
            XContentType.JSON
        );
        PutPipelineRequest ecsTrueRequest = new PutPipelineRequest(
            "ecs_true",
            new BytesArray(
                "{\n"
                    + "  \"description\" : \"This has ecs set to false\",\n"
                    + "  \"processors\" : [\n"
                    + "    {\n"
                    + "      \"user_agent\" : {\n"
                    + "        \"field\" : \"agent\",\n"
                    + "        \"ecs\" : true\n"
                    + "      }\n"
                    + "    }\n"
                    + "  ]\n"
                    + "}"
            ),
            XContentType.JSON
        );

        ClusterState state = ClusterState.builder(new ClusterName("test")).build();
        state = IngestService.innerPut(ecsTrueRequest, state);
        state = IngestService.innerPut(ecsFalseRequest, state);
        state = IngestService.innerPut(ecsNullRequest, state);

        final ClusterState finalState = state;
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(finalState));

        DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "The User-Agent ingest processor's ecs parameter is deprecated",
            "https://ela.st/es-deprecation-7-ingest-pipeline-ecs-option",
            "Remove the ecs parameter from your ingest pipelines. The User-Agent ingest processor always returns Elastic Common Schema "
                + "(ECS) fields in 8.0.",
            false,
            null
        );
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
            .metadata(
                Metadata.builder()
                    .put(
                        IndexTemplateMetadata.builder(tooManyFieldsTemplate)
                            .patterns(Collections.singletonList(randomAlphaOfLength(5)))
                            .putMapping("_doc", Strings.toString(badMappingBuilder))
                            .build()
                    )
                    .put(
                        IndexTemplateMetadata.builder(tooManyFieldsWithDefaultFieldsTemplate)
                            .patterns(Collections.singletonList(randomAlphaOfLength(5)))
                            .putMapping("_doc", Strings.toString(badMappingBuilder))
                            .settings(
                                Settings.builder()
                                    .put(
                                        IndexSettings.DEFAULT_FIELD_SETTING.getKey(),
                                        Collections.singletonList(randomAlphaOfLength(5)).toString()
                                    )
                            )
                            .build()
                    )
                    .put(
                        IndexTemplateMetadata.builder(goodTemplateName)
                            .patterns(Collections.singletonList(randomAlphaOfLength(5)))
                            .putMapping("_doc", Strings.toString(goodMappingBuilder))
                            .build()
                    )
                    .build()
            )
            .build();

        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(state));

        DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Fields in index template exceed automatic field expansion limit",
            "https://ela.st/es-deprecation-7-number-of-auto-expanded-fields",
            "Index templates "
                + Collections.singletonList(tooManyFieldsTemplate)
                + " have a number of fields which exceeds the "
                + "automatic field expansion limit of [1024] and does not have ["
                + IndexSettings.DEFAULT_FIELD_SETTING.getKey()
                + "] set, "
                + "which may cause queries which use automatic field expansion, such as query_string, simple_query_string, and multi_match "
                + "to fail if fields are not explicitly specified in the query.",
            false,
            null
        );
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
            .metadata(
                Metadata.builder()
                    .put(
                        IndexTemplateMetadata.builder(badTemplateName)
                            .patterns(Collections.singletonList(randomAlphaOfLength(5)))
                            .putMapping("_doc", Strings.toString(templateBuilder))
                            .build()
                    )
                    .build()
            )
            .build();

        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(state));
        if (expectIssue) {
            assertEquals(1, issues.size());
            DeprecationIssue issue = issues.get(0);
            assertEquals(DeprecationIssue.Level.WARNING, issue.getLevel());
            assertEquals("https://ela.st/es-deprecation-7-field_names-settings", issue.getUrl());
            assertEquals("Disabling the \"_field_names\" field in a template's index mappings is deprecated", issue.getMessage());
            assertEquals(
                "Remove the \"_field_names\" mapping that configures the enabled setting from the following templates: "
                    + "\""
                    + badTemplateName
                    + "\". There's no longer a need to disable this field to reduce index overhead if you have a lot "
                    + "of fields.",
                issue.getDetails()
            );
        } else {
            assertTrue(issues.isEmpty());
        }
    }

    public void testIndexTemplatesWithMultipleTypes() throws IOException {

        IndexTemplateMetadata multipleTypes = IndexTemplateMetadata.builder("multiple-types")
            .patterns(Collections.singletonList("foo"))
            .putMapping("type1", "{\"type1\":{}}")
            .putMapping("type2", "{\"type2\":{}}")
            .build();
        IndexTemplateMetadata singleType = IndexTemplateMetadata.builder("single-type")
            .patterns(Collections.singletonList("foo"))
            .putMapping("_doc", "{\"type1\":{}}")
            .build();
        ImmutableOpenMap<String, IndexTemplateMetadata> templates = ImmutableOpenMap.<String, IndexTemplateMetadata>builder()
            .fPut("multiple-types", multipleTypes)
            .fPut("single-type", singleType)
            .build();
        Metadata badMetadata = Metadata.builder().templates(templates).build();
        ClusterState badState = ClusterState.builder(new ClusterName("test")).metadata(badMetadata).build();
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(badState));
        assertThat(issues, hasSize(1));
        assertThat(
            issues.get(0).getDetails(),
            equalTo(
                "Update or remove the following index templates before upgrading to 8.0: [multiple-types]. See "
                    + "https://ela.st/es-deprecation-7-removal-of-types for alternatives to mapping types."
            )
        );
        assertWarnings(
            "Index template multiple-types contains multiple typed mappings;" + " templates in 8x will only support a single mapping"
        );

        Metadata goodMetadata = Metadata.builder()
            .templates(ImmutableOpenMap.<String, IndexTemplateMetadata>builder().fPut("single-type", singleType).build())
            .build();
        ClusterState goodState = ClusterState.builder(new ClusterName("test")).metadata(goodMetadata).build();
        assertThat(DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(goodState)), hasSize(0));
    }

    public void testIndexTemplatesWithCustomTypes() throws IOException {
        IndexTemplateMetadata template1 = IndexTemplateMetadata.builder("template1")
            .patterns(Collections.singletonList("foo"))
            .putMapping("type1", "{\"type1\":{}}")
            .build();
        IndexTemplateMetadata template2 = IndexTemplateMetadata.builder("template2")
            .patterns(Collections.singletonList("foo"))
            .putMapping("type2", "{\"type2\":{}}")
            .build();
        IndexTemplateMetadata template3 = IndexTemplateMetadata.builder("template3")
            .patterns(Collections.singletonList("foo"))
            .putMapping("_doc", "{\"_doc\":{}}")
            .build();
        // The following 3 have a custom type, but they should be ignored:
        IndexTemplateMetadata triggeredWatches = IndexTemplateMetadata.builder(".triggered_watches")
            .patterns(Collections.singletonList("foo"))
            .putMapping("someType", "{\"type2\":{}}")
            .build();
        IndexTemplateMetadata watchHistory9 = IndexTemplateMetadata.builder(".watch-history-9")
            .patterns(Collections.singletonList("foo"))
            .putMapping("someType", "{\"type2\":{}}")
            .build();
        IndexTemplateMetadata watches = IndexTemplateMetadata.builder(".watches")
            .patterns(Collections.singletonList("foo"))
            .putMapping("someType", "{\"type2\":{}}")
            .build();
        ImmutableOpenMap<String, IndexTemplateMetadata> templates = ImmutableOpenMap.<String, IndexTemplateMetadata>builder()
            .fPut("template1", template1)
            .fPut("template2", template2)
            .fPut("template3", template3)
            .fPut(".triggered_watches", triggeredWatches)
            .fPut(".watch-history-9", watchHistory9)
            .fPut(".watches", watches)
            .build();
        Metadata badMetadata = Metadata.builder().templates(templates).build();
        ClusterState badState = ClusterState.builder(new ClusterName("test")).metadata(badMetadata).build();
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(badState));
        assertThat(issues, hasSize(1));
        assertThat(
            issues.get(0).getDetails(),
            equalTo(
                "Update or remove the following index templates before upgrading to 8.0: [template1, template2]. See "
                    + "https://ela.st/es-deprecation-7-removal-of-types for alternatives to mapping types."
            )
        );
    }

    public void testCheckGeoShapeMappings() throws Exception {
        // First, testing only an index template:
        IndexTemplateMetadata indexTemplateMetadata = IndexTemplateMetadata.builder("single-type")
            .patterns(Collections.singletonList("foo"))
            .putMapping(
                "_doc",
                "{\n"
                    + "   \"_doc\":{\n"
                    + "      \"properties\":{\n"
                    + "         \"nested_field\":{\n"
                    + "            \"type\":\"nested\",\n"
                    + "            \"properties\":{\n"
                    + "               \"location\":{\n"
                    + "                  \"type\":\"geo_shape\",\n"
                    + "                  \"strategy\":\"recursive\",\n"
                    + "                  \"points_only\":true\n"
                    + "               }\n"
                    + "            }\n"
                    + "         }\n"
                    + "      }\n"
                    + "   }\n"
                    + "}"
            )
            .build();
        ImmutableOpenMap<String, IndexTemplateMetadata> templates = ImmutableOpenMap.<String, IndexTemplateMetadata>builder()
            .fPut("single-type", indexTemplateMetadata)
            .build();
        Metadata badMetadata = Metadata.builder().templates(templates).build();
        ClusterState badState = ClusterState.builder(new ClusterName("test")).metadata(badMetadata).build();
        DeprecationIssue issue = ClusterDeprecationChecks.checkGeoShapeTemplates(badState);

        assertThat(
            issue,
            equalTo(
                new DeprecationIssue(
                    DeprecationIssue.Level.CRITICAL,
                    "[single-type] index template uses deprecated geo_shape properties",
                    "https://ela.st/es-deprecation-7-geo-shape-mappings",
                    "Remove the following deprecated geo_shape properties from the mappings: [parameter [points_only] in field [location]; "
                        + "parameter [strategy] in field [location]].",
                    false,
                    null
                )
            )
        );

        // Second, testing only a component template:
        String templateName = "my-template";
        Settings settings = Settings.builder().put("index.number_of_shards", 1).build();
        CompressedXContent mappings = new CompressedXContent(
            "{\"properties\":{\"location\":{\"type\":\"geo_shape\", " + "\"strategy\":\"recursive\", \"points_only\":true}}}"
        );
        AliasMetadata alias = AliasMetadata.builder("alias").writeIndex(true).build();
        Template template = new Template(settings, mappings, Collections.singletonMap("alias", alias));
        ComponentTemplate componentTemplate = new ComponentTemplate(template, 1L, new HashMap<>());
        badMetadata = Metadata.builder().componentTemplates(Collections.singletonMap(templateName, componentTemplate)).build();
        badState = ClusterState.builder(new ClusterName("test")).metadata(badMetadata).build();
        issue = ClusterDeprecationChecks.checkGeoShapeTemplates(badState);

        assertThat(
            issue,
            equalTo(
                new DeprecationIssue(
                    DeprecationIssue.Level.CRITICAL,
                    "[my-template] component template uses deprecated geo_shape properties",
                    "https://ela.st/es-deprecation-7-geo-shape-mappings",
                    "Remove the following deprecated geo_shape properties from the mappings: [parameter [points_only] in field [location]; "
                        + "parameter [strategy] in field [location]].",
                    false,
                    null
                )
            )
        );

        // Third, trying a component template and an index template:
        badMetadata = Metadata.builder()
            .componentTemplates(Collections.singletonMap(templateName, componentTemplate))
            .templates(templates)
            .build();
        badState = ClusterState.builder(new ClusterName("test")).metadata(badMetadata).build();
        issue = ClusterDeprecationChecks.checkGeoShapeTemplates(badState);

        assertThat(
            issue,
            equalTo(
                new DeprecationIssue(
                    DeprecationIssue.Level.CRITICAL,
                    "[my-template] component template and [single-type] index template use deprecated geo_shape properties",
                    "https://ela.st/es-deprecation-7-geo-shape-mappings",
                    "Remove the following deprecated geo_shape properties from the mappings: [my-template: [parameter [points_only] in"
                        + " field [location]; parameter [strategy] in field [location]]]; [single-type: [parameter [points_only] in field "
                        + "[location]; parameter [strategy] in field [location]]].",
                    false,
                    null
                )
            )
        );
    }

    public void testSparseVectorMappings() throws Exception {
        // First, testing only an index template:
        IndexTemplateMetadata indexTemplateMetadata = IndexTemplateMetadata.builder("single-type")
            .patterns(Collections.singletonList("foo"))
            .putMapping(
                "_doc",
                "{\n"
                    + "   \"_doc\":{\n"
                    + "      \"properties\":{\n"
                    + "         \"my_sparse_vector\":{\n"
                    + "            \"type\":\"sparse_vector\"\n"
                    + "         },\n"
                    + "         \"nested_field\":{\n"
                    + "            \"type\":\"nested\",\n"
                    + "            \"properties\":{\n"
                    + "               \"my_nested_sparse_vector\":{\n"
                    + "                  \"type\":\"sparse_vector\"\n"
                    + "               }\n"
                    + "            }\n"
                    + "         }\n"
                    + "      }\n"
                    + "   }\n"
                    + "}"
            )
            .build();
        ImmutableOpenMap<String, IndexTemplateMetadata> templates = ImmutableOpenMap.<String, IndexTemplateMetadata>builder()
            .fPut("single-type", indexTemplateMetadata)
            .build();
        Metadata badMetadata = Metadata.builder().templates(templates).build();
        ClusterState badState = ClusterState.builder(new ClusterName("test")).metadata(badMetadata).build();
        DeprecationIssue issue = ClusterDeprecationChecks.checkSparseVectorTemplates(badState);

        assertThat(
            issue,
            equalTo(
                new DeprecationIssue(
                    DeprecationIssue.Level.CRITICAL,
                    "[single-type] index template uses deprecated sparse_vector properties",
                    "https://ela.st/es-deprecation-7-sparse-vector",
                    "Remove the following deprecated sparse_vector properties from the mappings: [my_sparse_vector]; "
                        + "[my_nested_sparse_vector].",
                    false,
                    null
                )
            )
        );

        // Second, testing only a component template:
        String templateName = "my-template";
        Settings settings = Settings.builder().put("index.number_of_shards", 1).build();
        CompressedXContent mappings = new CompressedXContent("{\"properties\":{\"my_sparse_vector\":{\"type\":\"sparse_vector\"}}}");
        AliasMetadata alias = AliasMetadata.builder("alias").writeIndex(true).build();
        Template template = new Template(settings, mappings, Collections.singletonMap("alias", alias));
        ComponentTemplate componentTemplate = new ComponentTemplate(template, 1L, new HashMap<>());
        badMetadata = Metadata.builder().componentTemplates(Collections.singletonMap(templateName, componentTemplate)).build();
        badState = ClusterState.builder(new ClusterName("test")).metadata(badMetadata).build();
        issue = ClusterDeprecationChecks.checkSparseVectorTemplates(badState);

        assertThat(
            issue,
            equalTo(
                new DeprecationIssue(
                    DeprecationIssue.Level.CRITICAL,
                    "[my-template] component template uses deprecated sparse_vector properties",
                    "https://ela.st/es-deprecation-7-sparse-vector",
                    "Remove the following deprecated sparse_vector properties from the mappings: [my_sparse_vector].",
                    false,
                    null
                )
            )
        );

        // Third, trying a component template and an index template:
        badMetadata = Metadata.builder()
            .componentTemplates(Collections.singletonMap(templateName, componentTemplate))
            .templates(templates)
            .build();
        badState = ClusterState.builder(new ClusterName("test")).metadata(badMetadata).build();
        issue = ClusterDeprecationChecks.checkSparseVectorTemplates(badState);

        assertThat(
            issue,
            equalTo(
                new DeprecationIssue(
                    DeprecationIssue.Level.CRITICAL,
                    "[my-template] component template and [single-type] index template use deprecated sparse_vector properties",
                    "https://ela.st/es-deprecation-7-sparse-vector",
                    "Remove the following deprecated sparse_vector properties from the mappings: [my-template: "
                        + "[my_sparse_vector]]; [single-type: [my_sparse_vector]; [my_nested_sparse_vector]].",
                    false,
                    null
                )
            )
        );
    }

    public void testCheckILMFreezeActions() throws Exception {
        Map<String, LifecyclePolicyMetadata> policies = new HashMap<>();
        Map<String, Phase> phases1 = new HashMap<>();
        Map<String, LifecycleAction> coldActions = new HashMap<>();
        coldActions.put("freeze", null);
        Phase coldPhase = new Phase("cold", TimeValue.ZERO, coldActions);
        Phase somePhase = new Phase("somePhase", TimeValue.ZERO, null);
        phases1.put("cold", coldPhase);
        phases1.put("somePhase", somePhase);
        LifecyclePolicy policy1 = new LifecyclePolicy("policy1", phases1, null);
        LifecyclePolicyMetadata policy1Metadata = new LifecyclePolicyMetadata(policy1, null, 0, 0);
        policies.put("policy1", policy1Metadata);
        Map<String, Phase> phases2 = new HashMap<>();
        phases2.put("cold", coldPhase);
        LifecyclePolicy policy2 = new LifecyclePolicy("policy2", phases2, null);
        LifecyclePolicyMetadata policy2Metadata = new LifecyclePolicyMetadata(policy2, null, 0, 0);
        policies.put("policy2", policy2Metadata);
        Metadata.Custom lifecycle = new IndexLifecycleMetadata(policies, OperationMode.RUNNING);
        Metadata badMetadata = Metadata.builder().putCustom("index_lifecycle", lifecycle).build();
        ClusterState badState = ClusterState.builder(new ClusterName("test")).metadata(badMetadata).build();
        DeprecationIssue issue = ClusterDeprecationChecks.checkILMFreezeActions(badState);
        assertThat(
            issue,
            equalTo(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "ILM policies use the deprecated freeze action",
                    "https://ela.st/es-deprecation-7-frozen-indices",
                    "Remove the freeze action from ILM policies: [policy1,policy2]",
                    false,
                    null
                )
            )
        );
    }

    public void testCheckTransientSettingsExistence() {
        Settings persistentSettings = Settings.builder().put("xpack.monitoring.collection.enabled", true).build();

        Settings transientSettings = Settings.builder()
            .put("indices.recovery.max_bytes_per_sec", "20mb")
            .put("action.auto_create_index", true)
            .put("cluster.routing.allocation.enable", "primaries")
            .build();
        Metadata metadataWithTransientSettings = Metadata.builder()
            .persistentSettings(persistentSettings)
            .transientSettings(transientSettings)
            .build();

        ClusterState badState = ClusterState.builder(new ClusterName("test")).metadata(metadataWithTransientSettings).build();
        DeprecationIssue issue = ClusterDeprecationChecks.checkTransientSettingsExistence(badState);
        assertThat(
            issue,
            equalTo(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "Transient cluster settings are deprecated",
                    "https://ela.st/es-deprecation-7-transient-cluster-settings",
                    "Use persistent settings to configure your cluster.",
                    false,
                    null
                )
            )
        );

        persistentSettings = Settings.builder().put("indices.recovery.max_bytes_per_sec", "20mb").build();
        Metadata metadataWithoutTransientSettings = Metadata.builder().persistentSettings(persistentSettings).build();

        ClusterState okState = ClusterState.builder(new ClusterName("test")).metadata(metadataWithoutTransientSettings).build();
        issue = ClusterDeprecationChecks.checkTransientSettingsExistence(okState);
        assertNull(issue);
    }

    public void testTemplateWithChainedMultiField() throws IOException {
        String chainedMultiFieldTemplate = randomAlphaOfLength(5);
        String goodTemplateName = randomAlphaOfLength(7);

        // A template with chained multi-fields
        XContentBuilder badMappingBuilder = jsonBuilder();
        badMappingBuilder.startObject();
        {
            badMappingBuilder.startObject("_doc");
            {
                badMappingBuilder.startObject("properties");
                {
                    badMappingBuilder.startObject("data");
                    {
                        badMappingBuilder.field("type", "text");
                        badMappingBuilder.startObject("fields");
                        {
                            badMappingBuilder.startObject("raw");
                            {
                                badMappingBuilder.field("type", "keyword");
                                badMappingBuilder.startObject("fields");
                                {
                                    badMappingBuilder.startObject("invalid");
                                    {
                                        badMappingBuilder.field("type", "text");
                                    }
                                    badMappingBuilder.endObject();
                                }
                                badMappingBuilder.endObject();
                            }
                            badMappingBuilder.endObject();
                        }
                        badMappingBuilder.endObject();
                    }
                    badMappingBuilder.endObject();
                }
                badMappingBuilder.endObject();
            }
            badMappingBuilder.endObject();
        }
        badMappingBuilder.endObject();

        // A template with an OK number of fields
        XContentBuilder goodMappingBuilder = jsonBuilder();
        goodMappingBuilder.startObject();
        {
            goodMappingBuilder.startObject("_doc");
            {
                goodMappingBuilder.startObject("properties");
                {
                    goodMappingBuilder.startObject("data");
                    {
                        goodMappingBuilder.field("type", "text");
                        goodMappingBuilder.startObject("fields");
                        {
                            goodMappingBuilder.startObject("raw");
                            {
                                goodMappingBuilder.field("type", "keyword");
                            }
                            goodMappingBuilder.endObject();
                        }
                        goodMappingBuilder.endObject();
                    }
                    goodMappingBuilder.endObject();
                }
                goodMappingBuilder.endObject();
            }
            goodMappingBuilder.endObject();
        }
        goodMappingBuilder.endObject();

        final ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(5)))
            .metadata(
                Metadata.builder()
                    .put(
                        IndexTemplateMetadata.builder(chainedMultiFieldTemplate)
                            .patterns(Collections.singletonList(randomAlphaOfLength(5)))
                            .putMapping("_doc", Strings.toString(badMappingBuilder))
                            .build()
                    )
                    .put(
                        IndexTemplateMetadata.builder(goodTemplateName)
                            .patterns(Collections.singletonList(randomAlphaOfLength(5)))
                            .putMapping("_doc", Strings.toString(goodMappingBuilder))
                            .build()
                    )
                    .build()
            )
            .build();

        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(state));

        DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Defining multi-fields within multi-fields on index template mappings is deprecated",
            "https://ela.st/es-deprecation-7-chained-multi-fields",
            "Remove chained multi-fields from the \""
                + chainedMultiFieldTemplate
                + "\" template. Multi-fields within multi-fields are not supported in 8.0.",
            false,
            null
        );
        assertEquals(singletonList(expected), issues);
    }

    public void testTemplateWithChainedMultiFieldDynamicTemplate() throws IOException {
        String chainedMultiFieldTemplate = randomAlphaOfLength(5);
        String dynamicTemplateName = randomAlphaOfLength(6);
        String goodTemplateName = randomAlphaOfLength(7);

        // A template with chained multi-fields
        XContentBuilder badMappingBuilder = jsonBuilder();
        badMappingBuilder.startObject();
        {
            badMappingBuilder.startObject("_doc");
            {
                badMappingBuilder.startObject("properties");
                {
                    badMappingBuilder.startObject("data");
                    {
                        badMappingBuilder.field("type", "text");
                    }
                    badMappingBuilder.endObject();
                }
                badMappingBuilder.endObject();
                badMappingBuilder.startArray("dynamic_templates");
                {
                    badMappingBuilder.startObject();
                    {
                        badMappingBuilder.startObject(dynamicTemplateName);
                        {
                            badMappingBuilder.field("match_mapping_type", "long");
                            badMappingBuilder.startObject("mapping");
                            {
                                badMappingBuilder.field("type", "long");
                                badMappingBuilder.startObject("fields");
                                {
                                    badMappingBuilder.startObject("stringified");
                                    {
                                        badMappingBuilder.field("type", "keyword");
                                        badMappingBuilder.startObject("fields");
                                        {
                                            badMappingBuilder.startObject("invalid");
                                            {
                                                badMappingBuilder.field("type", "text");
                                            }
                                            badMappingBuilder.endObject();
                                        }
                                        badMappingBuilder.endObject();
                                    }
                                    badMappingBuilder.endObject();
                                }
                                badMappingBuilder.endObject();
                            }
                            badMappingBuilder.endObject();
                        }
                        badMappingBuilder.endObject();
                    }
                    badMappingBuilder.endObject();
                }
                badMappingBuilder.endArray();
            }
            badMappingBuilder.endObject();
        }
        badMappingBuilder.endObject();

        // A template with an OK number of fields
        XContentBuilder goodMappingBuilder = jsonBuilder();
        goodMappingBuilder.startObject();
        {
            goodMappingBuilder.startObject("_doc");
            {
                goodMappingBuilder.startObject("properties");
                {
                    goodMappingBuilder.startObject("data");
                    {
                        goodMappingBuilder.field("type", "text");
                    }
                    goodMappingBuilder.endObject();
                }
                goodMappingBuilder.endObject();
                goodMappingBuilder.startArray("dynamic_templates");
                {
                    goodMappingBuilder.startObject();
                    {
                        goodMappingBuilder.startObject(dynamicTemplateName);
                        {
                            goodMappingBuilder.field("match_mapping_type", "long");
                            goodMappingBuilder.startObject("mapping");
                            {
                                goodMappingBuilder.field("type", "long");
                                goodMappingBuilder.startObject("fields");
                                {
                                    goodMappingBuilder.startObject("stringified");
                                    {
                                        goodMappingBuilder.field("type", "keyword");
                                    }
                                    goodMappingBuilder.endObject();
                                }
                                goodMappingBuilder.endObject();
                            }
                            goodMappingBuilder.endObject();
                        }
                        goodMappingBuilder.endObject();
                    }
                    goodMappingBuilder.endObject();
                }
                goodMappingBuilder.endArray();
            }
            goodMappingBuilder.endObject();
        }
        goodMappingBuilder.endObject();

        final ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(5)))
            .metadata(
                Metadata.builder()
                    .put(
                        IndexTemplateMetadata.builder(chainedMultiFieldTemplate)
                            .patterns(Collections.singletonList(randomAlphaOfLength(5)))
                            .putMapping("_doc", Strings.toString(badMappingBuilder))
                            .build()
                    )
                    .put(
                        IndexTemplateMetadata.builder(goodTemplateName)
                            .patterns(Collections.singletonList(randomAlphaOfLength(5)))
                            .putMapping("_doc", Strings.toString(goodMappingBuilder))
                            .build()
                    )
                    .build()
            )
            .build();

        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(state));

        DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Defining multi-fields within multi-fields on index template dynamic_templates is deprecated",
            "https://ela.st/es-deprecation-7-chained-multi-fields",
            "Remove chained multi-fields from the \""
                + chainedMultiFieldTemplate
                + "\" template. Multi-fields within multi-fields are not supported in 8.0.",
            false,
            null
        );
        assertEquals(singletonList(expected), issues);
    }

    public void testComponentTemplateWithChainedMultiField() throws IOException {
        String chainedMultiFieldTemplate = randomAlphaOfLength(5);
        String goodTemplateName = randomAlphaOfLength(7);

        // A template with chained multi-fields
        XContentBuilder badMappingBuilder = jsonBuilder();
        badMappingBuilder.startObject();
        {
            badMappingBuilder.startObject("_doc");
            {
                badMappingBuilder.startObject("properties");
                {
                    badMappingBuilder.startObject("data");
                    {
                        badMappingBuilder.field("type", "text");
                        badMappingBuilder.startObject("fields");
                        {
                            badMappingBuilder.startObject("raw");
                            {
                                badMappingBuilder.field("type", "keyword");
                                badMappingBuilder.startObject("fields");
                                {
                                    badMappingBuilder.startObject("invalid");
                                    {
                                        badMappingBuilder.field("type", "text");
                                    }
                                    badMappingBuilder.endObject();
                                }
                                badMappingBuilder.endObject();
                            }
                            badMappingBuilder.endObject();
                        }
                        badMappingBuilder.endObject();
                    }
                    badMappingBuilder.endObject();
                }
                badMappingBuilder.endObject();
            }
            badMappingBuilder.endObject();
        }
        badMappingBuilder.endObject();

        // A template with an OK number of fields
        XContentBuilder goodMappingBuilder = jsonBuilder();
        goodMappingBuilder.startObject();
        {
            goodMappingBuilder.startObject("_doc");
            {
                goodMappingBuilder.startObject("properties");
                {
                    goodMappingBuilder.startObject("data");
                    {
                        goodMappingBuilder.field("type", "text");
                        goodMappingBuilder.startObject("fields");
                        {
                            goodMappingBuilder.startObject("raw");
                            {
                                goodMappingBuilder.field("type", "keyword");
                            }
                            goodMappingBuilder.endObject();
                        }
                        goodMappingBuilder.endObject();
                    }
                    goodMappingBuilder.endObject();
                }
                goodMappingBuilder.endObject();
            }
            goodMappingBuilder.endObject();
        }
        goodMappingBuilder.endObject();

        badMappingBuilder.flush();
        goodMappingBuilder.flush();

        final ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(5)))
            .metadata(
                Metadata.builder()
                    .put(
                        chainedMultiFieldTemplate,
                        new ComponentTemplate(
                            new Template(
                                null,
                                new CompressedXContent(((ByteArrayOutputStream) badMappingBuilder.getOutputStream()).toByteArray()),
                                null
                            ),
                            null,
                            null
                        )
                    )
                    .put(
                        goodTemplateName,
                        new ComponentTemplate(
                            new Template(
                                null,
                                new CompressedXContent(((ByteArrayOutputStream) goodMappingBuilder.getOutputStream()).toByteArray()),
                                null
                            ),
                            null,
                            null
                        )
                    )
                    .build()
            )
            .build();

        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(state));

        DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Defining multi-fields within multi-fields on component templates is deprecated",
            "https://ela.st/es-deprecation-7-chained-multi-fields",
            "Remove chained multi-fields from the \""
                + chainedMultiFieldTemplate
                + "\" component template. Multi-fields within multi-fields are not supported in 8.0.",
            false,
            null
        );
        assertEquals(singletonList(expected), issues);
    }

    public void testComponentTemplateWithChainedMultiFieldDynamicTemplate() throws IOException {
        String chainedMultiFieldTemplate = randomAlphaOfLength(5);
        String dynamicTemplateName = randomAlphaOfLength(6);
        String goodTemplateName = randomAlphaOfLength(7);

        // A template with chained multi-fields
        XContentBuilder badMappingBuilder = jsonBuilder();
        badMappingBuilder.startObject();
        {
            badMappingBuilder.startObject("_doc");
            {
                badMappingBuilder.startObject("properties");
                {
                    badMappingBuilder.startObject("data");
                    {
                        badMappingBuilder.field("type", "text");
                    }
                    badMappingBuilder.endObject();
                }
                badMappingBuilder.endObject();
                badMappingBuilder.startArray("dynamic_templates");
                {
                    badMappingBuilder.startObject();
                    {
                        badMappingBuilder.startObject(dynamicTemplateName);
                        {
                            badMappingBuilder.field("match_mapping_type", "long");
                            badMappingBuilder.startObject("mapping");
                            {
                                badMappingBuilder.field("type", "long");
                                badMappingBuilder.startObject("fields");
                                {
                                    badMappingBuilder.startObject("stringified");
                                    {
                                        badMappingBuilder.field("type", "keyword");
                                        badMappingBuilder.startObject("fields");
                                        {
                                            badMappingBuilder.startObject("invalid");
                                            {
                                                badMappingBuilder.field("type", "text");
                                            }
                                            badMappingBuilder.endObject();
                                        }
                                        badMappingBuilder.endObject();
                                    }
                                    badMappingBuilder.endObject();
                                }
                                badMappingBuilder.endObject();
                            }
                            badMappingBuilder.endObject();
                        }
                        badMappingBuilder.endObject();
                    }
                    badMappingBuilder.endObject();
                }
                badMappingBuilder.endArray();
            }
            badMappingBuilder.endObject();
        }
        badMappingBuilder.endObject();

        // A template with an OK number of fields
        XContentBuilder goodMappingBuilder = jsonBuilder();
        goodMappingBuilder.startObject();
        {
            goodMappingBuilder.startObject("_doc");
            {
                goodMappingBuilder.startObject("properties");
                {
                    goodMappingBuilder.startObject("data");
                    {
                        goodMappingBuilder.field("type", "text");
                    }
                    goodMappingBuilder.endObject();
                }
                goodMappingBuilder.endObject();
                goodMappingBuilder.startArray("dynamic_templates");
                {
                    goodMappingBuilder.startObject();
                    {
                        goodMappingBuilder.startObject(dynamicTemplateName);
                        {
                            goodMappingBuilder.field("match_mapping_type", "long");
                            goodMappingBuilder.startObject("mapping");
                            {
                                goodMappingBuilder.field("type", "long");
                                goodMappingBuilder.startObject("fields");
                                {
                                    goodMappingBuilder.startObject("stringified");
                                    {
                                        goodMappingBuilder.field("type", "keyword");
                                    }
                                    goodMappingBuilder.endObject();
                                }
                                goodMappingBuilder.endObject();
                            }
                            goodMappingBuilder.endObject();
                        }
                        goodMappingBuilder.endObject();
                    }
                    goodMappingBuilder.endObject();
                }
                goodMappingBuilder.endArray();
            }
            goodMappingBuilder.endObject();
        }
        goodMappingBuilder.endObject();

        badMappingBuilder.flush();
        goodMappingBuilder.flush();

        final ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(5)))
            .metadata(
                Metadata.builder()
                    .put(
                        chainedMultiFieldTemplate,
                        new ComponentTemplate(
                            new Template(
                                null,
                                new CompressedXContent(((ByteArrayOutputStream) badMappingBuilder.getOutputStream()).toByteArray()),
                                null
                            ),
                            null,
                            null
                        )
                    )
                    .put(
                        goodTemplateName,
                        new ComponentTemplate(
                            new Template(
                                null,
                                new CompressedXContent(((ByteArrayOutputStream) goodMappingBuilder.getOutputStream()).toByteArray()),
                                null
                            ),
                            null,
                            null
                        )
                    )
                    .build()
            )
            .build();

        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(state));

        DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Defining multi-fields within multi-fields on component template dynamic_templates is deprecated",
            "https://ela.st/es-deprecation-7-chained-multi-fields",
            "Remove chained multi-fields from the \""
                + chainedMultiFieldTemplate
                + "\" component template. Multi-fields within multi-fields are not supported in 8.0.",
            false,
            null
        );
        assertEquals(singletonList(expected), issues);
    }

    public void testTemplateWithBoostedField() throws IOException {
        String boostedFieldTemplate = randomAlphaOfLength(5);
        String goodTemplateName = randomAlphaOfLength(7);

        // A template with boosted field
        XContentBuilder badMappingBuilder = jsonBuilder();
        badMappingBuilder.startObject();
        {
            badMappingBuilder.startObject("_doc");
            {
                badMappingBuilder.startObject("properties");
                {
                    badMappingBuilder.startObject("data");
                    {
                        badMappingBuilder.field("type", "text");
                        badMappingBuilder.field("boost", 5.0);
                    }
                    badMappingBuilder.endObject();
                }
                badMappingBuilder.endObject();
            }
            badMappingBuilder.endObject();
        }
        badMappingBuilder.endObject();

        XContentBuilder goodMappingBuilder = jsonBuilder();
        goodMappingBuilder.startObject();
        {
            goodMappingBuilder.startObject("_doc");
            {
                goodMappingBuilder.startObject("properties");
                {
                    goodMappingBuilder.startObject("data");
                    {
                        goodMappingBuilder.field("type", "text");
                    }
                    goodMappingBuilder.endObject();
                }
                goodMappingBuilder.endObject();
            }
            goodMappingBuilder.endObject();
        }
        goodMappingBuilder.endObject();

        final ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(5)))
            .metadata(
                Metadata.builder()
                    .put(
                        IndexTemplateMetadata.builder(boostedFieldTemplate)
                            .patterns(Collections.singletonList(randomAlphaOfLength(5)))
                            .putMapping("_doc", Strings.toString(badMappingBuilder))
                            .build()
                    )
                    .put(
                        IndexTemplateMetadata.builder(goodTemplateName)
                            .patterns(Collections.singletonList(randomAlphaOfLength(5)))
                            .putMapping("_doc", Strings.toString(goodMappingBuilder))
                            .build()
                    )
                    .build()
            )
            .build();

        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(state));

        DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Defining boosted fields on index template mappings is deprecated",
            "https://ela.st/es-deprecation-7-boost-fields",
            "Remove boost fields from the \""
                + boostedFieldTemplate
                + "\" template. Configuring a boost value on mapping fields is not supported in 8.0.",
            false,
            null
        );
        assertEquals(singletonList(expected), issues);
    }

    public void testTemplateWithBoostedFieldDynamicTemplate() throws IOException {
        String boostedFieldTemplate = randomAlphaOfLength(5);
        String dynamicTemplateName = randomAlphaOfLength(6);
        String goodTemplateName = randomAlphaOfLength(7);

        // A template with boosted fields
        XContentBuilder badMappingBuilder = jsonBuilder();
        badMappingBuilder.startObject();
        {
            badMappingBuilder.startObject("_doc");
            {
                badMappingBuilder.startObject("properties");
                {
                    badMappingBuilder.startObject("data");
                    {
                        badMappingBuilder.field("type", "text");
                    }
                    badMappingBuilder.endObject();
                }
                badMappingBuilder.endObject();
                badMappingBuilder.startArray("dynamic_templates");
                {
                    badMappingBuilder.startObject();
                    {
                        badMappingBuilder.startObject(dynamicTemplateName);
                        {
                            badMappingBuilder.field("match_mapping_type", "long");
                            badMappingBuilder.startObject("mapping");
                            {
                                badMappingBuilder.field("type", "long");
                                badMappingBuilder.field("boost", 5.0);
                            }
                            badMappingBuilder.endObject();
                        }
                        badMappingBuilder.endObject();
                    }
                    badMappingBuilder.endObject();
                }
                badMappingBuilder.endArray();
            }
            badMappingBuilder.endObject();
        }
        badMappingBuilder.endObject();

        XContentBuilder goodMappingBuilder = jsonBuilder();
        goodMappingBuilder.startObject();
        {
            goodMappingBuilder.startObject("_doc");
            {
                goodMappingBuilder.startObject("properties");
                {
                    goodMappingBuilder.startObject("data");
                    {
                        goodMappingBuilder.field("type", "text");
                    }
                    goodMappingBuilder.endObject();
                }
                goodMappingBuilder.endObject();
                goodMappingBuilder.startArray("dynamic_templates");
                {
                    goodMappingBuilder.startObject();
                    {
                        goodMappingBuilder.startObject(dynamicTemplateName);
                        {
                            goodMappingBuilder.field("match_mapping_type", "long");
                            goodMappingBuilder.startObject("mapping");
                            {
                                goodMappingBuilder.field("type", "long");
                            }
                            goodMappingBuilder.endObject();
                        }
                        goodMappingBuilder.endObject();
                    }
                    goodMappingBuilder.endObject();
                }
                goodMappingBuilder.endArray();
            }
            goodMappingBuilder.endObject();
        }
        goodMappingBuilder.endObject();

        final ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(5)))
            .metadata(
                Metadata.builder()
                    .put(
                        IndexTemplateMetadata.builder(boostedFieldTemplate)
                            .patterns(Collections.singletonList(randomAlphaOfLength(5)))
                            .putMapping("_doc", Strings.toString(badMappingBuilder))
                            .build()
                    )
                    .put(
                        IndexTemplateMetadata.builder(goodTemplateName)
                            .patterns(Collections.singletonList(randomAlphaOfLength(5)))
                            .putMapping("_doc", Strings.toString(goodMappingBuilder))
                            .build()
                    )
                    .build()
            )
            .build();

        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(state));

        DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Defining boosted fields on index template dynamic_templates is deprecated",
            "https://ela.st/es-deprecation-7-boost-fields",
            "Remove boost fields from the \""
                + boostedFieldTemplate
                + "\" template. Configuring a boost value on mapping fields is not supported in 8.0.",
            false,
            null
        );
        assertEquals(singletonList(expected), issues);
    }

    public void testComponentTemplateWithBoostedField() throws IOException {
        String boostedFieldTemplate = randomAlphaOfLength(5);
        String goodTemplateName = randomAlphaOfLength(7);

        // A template with boosted fields
        XContentBuilder badMappingBuilder = jsonBuilder();
        badMappingBuilder.startObject();
        {
            badMappingBuilder.startObject("_doc");
            {
                badMappingBuilder.startObject("properties");
                {
                    badMappingBuilder.startObject("data");
                    {
                        badMappingBuilder.field("type", "text");
                        badMappingBuilder.field("boost", 5.0);
                    }
                    badMappingBuilder.endObject();
                }
                badMappingBuilder.endObject();
            }
            badMappingBuilder.endObject();
        }
        badMappingBuilder.endObject();

        XContentBuilder goodMappingBuilder = jsonBuilder();
        goodMappingBuilder.startObject();
        {
            goodMappingBuilder.startObject("_doc");
            {
                goodMappingBuilder.startObject("properties");
                {
                    goodMappingBuilder.startObject("data");
                    {
                        goodMappingBuilder.field("type", "text");
                    }
                    goodMappingBuilder.endObject();
                }
                goodMappingBuilder.endObject();
            }
            goodMappingBuilder.endObject();
        }
        goodMappingBuilder.endObject();

        badMappingBuilder.flush();
        goodMappingBuilder.flush();

        final ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(5)))
            .metadata(
                Metadata.builder()
                    .put(
                        boostedFieldTemplate,
                        new ComponentTemplate(
                            new Template(
                                null,
                                new CompressedXContent(((ByteArrayOutputStream) badMappingBuilder.getOutputStream()).toByteArray()),
                                null
                            ),
                            null,
                            null
                        )
                    )
                    .put(
                        goodTemplateName,
                        new ComponentTemplate(
                            new Template(
                                null,
                                new CompressedXContent(((ByteArrayOutputStream) goodMappingBuilder.getOutputStream()).toByteArray()),
                                null
                            ),
                            null,
                            null
                        )
                    )
                    .build()
            )
            .build();

        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(state));

        DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Defining boosted fields on component templates is deprecated",
            "https://ela.st/es-deprecation-7-boost-fields",
            "Remove boost fields from the \""
                + boostedFieldTemplate
                + "\" component template. Configuring a boost value on mapping fields is not supported in 8.0.",
            false,
            null
        );
        assertEquals(singletonList(expected), issues);
    }

    public void testComponentTemplateWithBoostedFieldDynamicTemplate() throws IOException {
        String boostedFieldTemplate = randomAlphaOfLength(5);
        String dynamicTemplateName = randomAlphaOfLength(6);
        String goodTemplateName = randomAlphaOfLength(7);

        // A template with boosted fields
        XContentBuilder badMappingBuilder = jsonBuilder();
        badMappingBuilder.startObject();
        {
            badMappingBuilder.startObject("_doc");
            {
                badMappingBuilder.startObject("properties");
                {
                    badMappingBuilder.startObject("data");
                    {
                        badMappingBuilder.field("type", "text");
                    }
                    badMappingBuilder.endObject();
                }
                badMappingBuilder.endObject();
                badMappingBuilder.startArray("dynamic_templates");
                {
                    badMappingBuilder.startObject();
                    {
                        badMappingBuilder.startObject(dynamicTemplateName);
                        {
                            badMappingBuilder.field("match_mapping_type", "long");
                            badMappingBuilder.startObject("mapping");
                            {
                                badMappingBuilder.field("type", "long");
                                badMappingBuilder.field("boost", 5.0);
                            }
                            badMappingBuilder.endObject();
                        }
                        badMappingBuilder.endObject();
                    }
                    badMappingBuilder.endObject();
                }
                badMappingBuilder.endArray();
            }
            badMappingBuilder.endObject();
        }
        badMappingBuilder.endObject();

        XContentBuilder goodMappingBuilder = jsonBuilder();
        goodMappingBuilder.startObject();
        {
            goodMappingBuilder.startObject("_doc");
            {
                goodMappingBuilder.startObject("properties");
                {
                    goodMappingBuilder.startObject("data");
                    {
                        goodMappingBuilder.field("type", "text");
                    }
                    goodMappingBuilder.endObject();
                }
                goodMappingBuilder.endObject();
                goodMappingBuilder.startArray("dynamic_templates");
                {
                    goodMappingBuilder.startObject();
                    {
                        goodMappingBuilder.startObject(dynamicTemplateName);
                        {
                            goodMappingBuilder.field("match_mapping_type", "long");
                            goodMappingBuilder.startObject("mapping");
                            {
                                goodMappingBuilder.field("type", "long");
                            }
                            goodMappingBuilder.endObject();
                        }
                        goodMappingBuilder.endObject();
                    }
                    goodMappingBuilder.endObject();
                }
                goodMappingBuilder.endArray();
            }
            goodMappingBuilder.endObject();
        }
        goodMappingBuilder.endObject();

        badMappingBuilder.flush();
        goodMappingBuilder.flush();

        final ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(5)))
            .metadata(
                Metadata.builder()
                    .put(
                        boostedFieldTemplate,
                        new ComponentTemplate(
                            new Template(
                                null,
                                new CompressedXContent(((ByteArrayOutputStream) badMappingBuilder.getOutputStream()).toByteArray()),
                                null
                            ),
                            null,
                            null
                        )
                    )
                    .put(
                        goodTemplateName,
                        new ComponentTemplate(
                            new Template(
                                null,
                                new CompressedXContent(((ByteArrayOutputStream) goodMappingBuilder.getOutputStream()).toByteArray()),
                                null
                            ),
                            null,
                            null
                        )
                    )
                    .build()
            )
            .build();

        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(state));

        DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Defining boosted fields on component template dynamic_templates is deprecated",
            "https://ela.st/es-deprecation-7-boost-fields",
            "Remove boost fields from the \""
                + boostedFieldTemplate
                + "\" component template. Configuring a boost value on mapping fields is not supported in 8.0.",
            false,
            null
        );
        assertEquals(singletonList(expected), issues);
    }

    public void testEmptyDataTierPreference() {
        List<String> indexNames = new ArrayList<>();
        indexNames.add(".some_dotted_index");
        for (int i = 0; i < 15; i++) {
            indexNames.add("test-index-name-" + i);
        }

        List<IndexMetadata> indices = new ArrayList<>();
        for (String indexName : indexNames) {
            indices.add(
                IndexMetadata.builder(indexName)
                    .settings(settings(Version.CURRENT).put(DataTier.TIER_PREFERENCE_SETTING.getKey(), "  "))
                    .numberOfShards(randomIntBetween(1, 100))
                    .numberOfReplicas(randomIntBetween(1, 100))
                    .build()
            );
        }

        {
            List<DeprecationIssue> issues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(ClusterState.EMPTY_STATE));
            assertThat(issues, empty());
        }

        {
            Metadata.Builder metadata = Metadata.builder();
            for (IndexMetadata indexMetadata : indices) {
                metadata.put(indexMetadata, false);
            }
            ClusterState clusterState = ClusterState.builder(clusterStateWithoutAllDataRoles()).metadata(metadata).build();

            List<DeprecationIssue> issues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(clusterState));
            assertThat(
                issues,
                contains(
                    new DeprecationIssue(
                        DeprecationIssue.Level.WARNING,
                        "No [index.routing.allocation.include._tier_preference] is set for indices [test-index-name-0, "
                            + "test-index-name-1, test-index-name-10, test-index-name-11, test-index-name-12, test-index-name-13, "
                            + "test-index-name-14, test-index-name-2, test-index-name-3, test-index-name-4, test-index-name-5, "
                            + "test-index-name-6, test-index-name-7, test-index-name-8, ... (16 in total, 2 omitted)].",
                        "https://ela.st/es-deprecation-7-empty-tier-preference",
                        "Specify a data tier preference for these indices.",
                        false,
                        null
                    )
                )
            );
        }
    }

    public void testCheckShards() {
        /*
         * This test sets the number of allowed shards per node to 5 and creates 2 nodes. So we have room for 10 shards, which is the
         * number of shards that checkShards() is making sure we can add. The first time there are no indices, so the check passes. The
         * next time there is an index with one shard and one replica, leaving room for 8 shards. So the check fails.
         */
        final ClusterState state = ClusterState.builder(new ClusterName(randomAlphaOfLength(5)))
            .metadata(
                Metadata.builder()
                    .persistentSettings(Settings.builder().put(ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), 5).build())
                    .build()
            )
            .nodes(
                DiscoveryNodes.builder()
                    .add(new DiscoveryNode(UUID.randomUUID().toString(), buildNewFakeTransportAddress(), Version.CURRENT))
                    .add(new DiscoveryNode(UUID.randomUUID().toString(), buildNewFakeTransportAddress(), Version.CURRENT))
            )
            .build();
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(state));
        assertThat(0, equalTo(issues.size()));

        final ClusterState stateWithProblems = ClusterState.builder(new ClusterName(randomAlphaOfLength(5)))
            .metadata(
                Metadata.builder()
                    .persistentSettings(Settings.builder().put(ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), 4).build())
                    .put(
                        IndexMetadata.builder(randomAlphaOfLength(10))
                            .settings(settings(Version.CURRENT).put(DataTier.TIER_PREFERENCE_SETTING.getKey(), "  "))
                            .numberOfShards(1)
                            .numberOfReplicas(1)
                            .build(),
                        false
                    )
                    .build()
            )
            .nodes(
                DiscoveryNodes.builder()
                    .add(new DiscoveryNode(UUID.randomUUID().toString(), buildNewFakeTransportAddress(), Version.CURRENT))
                    .add(new DiscoveryNode(UUID.randomUUID().toString(), buildNewFakeTransportAddress(), Version.CURRENT))
            )
            .build();

        issues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(stateWithProblems));

        DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "The cluster has too many shards to be able to upgrade",
            "https://ela.st/es-deprecation-7-shard-limit",
            "Upgrading requires adding a small number of new shards. There is not enough room for 10 more shards. Increase the cluster"
                + ".max_shards_per_node setting, or remove indices to clear up resources.",
            false,
            null
        );
        assertEquals(singletonList(expected), issues);
    }

    private static ClusterState clusterStateWithoutAllDataRoles() {
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        List<DiscoveryNode> nodesList = org.elasticsearch.core.List.of(
            new DiscoveryNode(
                "name_0",
                "node_0",
                buildNewFakeTransportAddress(),
                org.elasticsearch.core.Map.of(),
                org.elasticsearch.core.Set.of(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE),
                Version.CURRENT
            )
        );
        for (DiscoveryNode node : nodesList) {
            discoBuilder = discoBuilder.add(node);
        }
        discoBuilder.localNodeId(randomFrom(nodesList).getId());
        discoBuilder.masterNodeId(randomFrom(nodesList).getId());

        return ClusterState.builder(ClusterState.EMPTY_STATE).nodes(discoBuilder.build()).build();
    }
}
