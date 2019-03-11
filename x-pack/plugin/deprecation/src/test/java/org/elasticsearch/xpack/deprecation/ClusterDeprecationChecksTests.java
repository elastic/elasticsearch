/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.Version;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.discovery.DiscoverySettings.NO_MASTER_BLOCK_SETTING;
import static org.elasticsearch.xpack.deprecation.DeprecationChecks.CLUSTER_SETTINGS_CHECKS;
import static org.elasticsearch.xpack.deprecation.IndexDeprecationChecksTests.addRandomFields;

public class ClusterDeprecationChecksTests extends ESTestCase {

    public void testCheckClusterName() {
        final String badClusterName = randomAlphaOfLengthBetween(0, 10) + ":" + randomAlphaOfLengthBetween(0, 10);
        final ClusterState badClusterState = ClusterState.builder(new ClusterName(badClusterName)).build();

        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.CRITICAL, "Cluster name cannot contain ':'",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                "#_literal_literal_is_no_longer_allowed_in_cluster_name",
            "This cluster is named [" + badClusterName + "], which contains the illegal character ':'.");
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(badClusterState));
        assertEquals(singletonList(expected), issues);

        final String goodClusterName = randomAlphaOfLengthBetween(1,30);
        final ClusterState goodClusterState = ClusterState.builder(new ClusterName(goodClusterName)).build();
        List<DeprecationIssue> noIssues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(goodClusterState));
        assertTrue(noIssues.isEmpty());
    }

    public void testCheckNoMasterBlock() {
        MetaData metaData = MetaData.builder()
            .persistentSettings(Settings.builder()
                .put(NO_MASTER_BLOCK_SETTING.getKey(), randomFrom("all", "write"))
                .build())
            .build();
        ClusterState state = ClusterState.builder(new ClusterName("test"))
            .metaData(metaData)
            .build();
        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.WARNING,
            "Master block setting will be renamed",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                "#_new_name_for_literal_no_master_block_literal_setting",
            "The setting discovery.zen.no_master_block will be renamed to cluster.no_master_block in 7.0. " +
                "Please unset discovery.zen.no_master_block and set cluster.no_master_block after upgrading to 7.0.");
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(state));
        assertEquals(singletonList(expected), issues);
    }

    public void testCheckShardLimit() {
        int shardsPerNode = randomIntBetween(2, 10000);
        int nodeCount = randomIntBetween(1, 10);
        int maxShardsInCluster = shardsPerNode * nodeCount;
        int currentOpenShards = maxShardsInCluster + randomIntBetween(0, 100);

        DiscoveryNodes.Builder discoveryNodesBuilder = DiscoveryNodes.builder();
        for (int i = 0; i < nodeCount; i++) {
            DiscoveryNode discoveryNode = DiscoveryNode.createLocal(Settings.builder().put("node.name", "node_check" + i).build(),
                new TransportAddress(TransportAddress.META_ADDRESS, 9200 + i), "test" + i);
            discoveryNodesBuilder.add(discoveryNode);
        }

        // verify deprecation issue is returned when number of open shards exceeds cluster soft limit
        MetaData metaData = MetaData.builder().put(IndexMetaData.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(currentOpenShards)
            .numberOfReplicas(0))
            .persistentSettings(settings(Version.CURRENT)
                .put(MetaData.SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), String.valueOf(shardsPerNode)).build())
            .build();
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(metaData).nodes(discoveryNodesBuilder).build();
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(state));
        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.WARNING,
            "Number of open shards exceeds cluster soft limit",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                "#_cluster_wide_shard_soft_limit",
            "There are [" + currentOpenShards + "] open shards in this cluster, but the cluster is limited to [" +
                shardsPerNode + "] per data node, for [" + maxShardsInCluster + "] maximum.");
        assertEquals(singletonList(expected), issues);

        // verify no deprecation issues are returned when number of open shards is below the cluster soft limit
        MetaData goodMetaData = MetaData.builder(metaData).put(IndexMetaData.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfReplicas(0)
            .numberOfShards(maxShardsInCluster - randomIntBetween(1, (maxShardsInCluster - 1)))).build();
        ClusterState goodState = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(goodMetaData).nodes(state.nodes()).build();
        issues = DeprecationChecks.filterChecks(CLUSTER_SETTINGS_CHECKS, c -> c.apply(goodState));
        assertTrue(issues.isEmpty());
    }

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
            "User-Agent ingest plugin will use ECS-formatted output",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                "#ingest-user-agent-ecs-always",
            "Ingest pipelines [ecs_null] will change to using ECS output format by default in 7.0");
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
                        .put(IndexSettings.DEFAULT_FIELD_SETTING_KEY,
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
                "automatic field expansion limit of [1024] and does not have [" + IndexSettings.DEFAULT_FIELD_SETTING_KEY + "] set, " +
                "which may cause queries which use automatic field expansion, such as query_string, simple_query_string, and multi_match " +
                "to fail if fields are not explicitly specified in the query.");
        assertEquals(singletonList(expected), issues);
    }
}
