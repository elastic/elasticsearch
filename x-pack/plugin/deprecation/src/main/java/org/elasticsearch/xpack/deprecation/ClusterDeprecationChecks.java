/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.deprecation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.elasticsearch.discovery.DiscoverySettings.NO_MASTER_BLOCK_SETTING;
import static org.elasticsearch.search.SearchModule.INDICES_MAX_CLAUSE_COUNT_SETTING;

public class ClusterDeprecationChecks {
    private static final Logger logger = LogManager.getLogger(ClusterDeprecationChecks.class);

    static DeprecationIssue checkShardLimit(ClusterState state) {
        int shardsPerNode = MetaData.SETTING_CLUSTER_MAX_SHARDS_PER_NODE.get(state.metaData().settings());
        int nodeCount = state.getNodes().getDataNodes().size();
        int maxShardsInCluster = shardsPerNode * nodeCount;
        int currentOpenShards = state.getMetaData().getTotalOpenIndexShards();

        if (nodeCount > 0 && currentOpenShards >= maxShardsInCluster) {
            return new DeprecationIssue(DeprecationIssue.Level.WARNING,
                "Number of open shards exceeds cluster soft limit",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                    "#_cluster_wide_shard_soft_limit",
                "There are [" + currentOpenShards + "] open shards in this cluster, but the cluster is limited to [" +
                    shardsPerNode + "] per data node, for [" + maxShardsInCluster + "] maximum.");
        }
        return null;
    }

    static DeprecationIssue checkNoMasterBlock(ClusterState state) {
        if (state.metaData().settings().hasValue(NO_MASTER_BLOCK_SETTING.getKey())) {
            return new DeprecationIssue(DeprecationIssue.Level.WARNING,
                "Master block setting will be renamed",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                    "#_new_name_for_literal_no_master_block_literal_setting",
                "The setting discovery.zen.no_master_block will be renamed to cluster.no_master_block in 7.0. " +
                    "Please unset discovery.zen.no_master_block and set cluster.no_master_block after upgrading to 7.0.");
        }
        return null;
    }

    static DeprecationIssue checkClusterName(ClusterState state) {
        String clusterName = state.getClusterName().value();
        if (clusterName.contains(":")) {
            return new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                "Cluster name cannot contain ':'",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                    "#_literal_literal_is_no_longer_allowed_in_cluster_name",
                "This cluster is named [" + clusterName + "], which contains the illegal character ':'.");
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    static DeprecationIssue checkUserAgentPipelines(ClusterState state) {
        List<PipelineConfiguration> pipelines = IngestService.getPipelines(state);

        List<String> pipelinesWithDeprecatedEcsConfig = pipelines.stream()
            .filter(Objects::nonNull)
            .filter(pipeline -> {
                Map<String, Object> pipelineConfig = pipeline.getConfigAsMap();

                List<Map<String, Map<String, Object>>> processors =
                    (List<Map<String, Map<String, Object>>>) pipelineConfig.get("processors");
                return processors.stream()
                    .filter(Objects::nonNull)
                    .filter(processor -> processor.containsKey("user_agent"))
                    .map(processor -> processor.get("user_agent"))
                    .anyMatch(processorConfig -> processorConfig.containsKey("ecs") == false);
            })
            .map(PipelineConfiguration::getId)
            .sorted() // Make the warning consistent for testing purposes
            .collect(Collectors.toList());
        if (pipelinesWithDeprecatedEcsConfig.isEmpty() == false) {
            return new DeprecationIssue(DeprecationIssue.Level.WARNING,
                "User-Agent ingest plugin will use ECS-formatted output",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                    "#ingest-user-agent-ecs-always",
                "Ingest pipelines " + pipelinesWithDeprecatedEcsConfig + " will change to using ECS output format by default in 7.0");
        }
        return null;

    }

    static DeprecationIssue checkTemplatesWithTooManyFields(ClusterState state) {
        Integer maxClauseCount = INDICES_MAX_CLAUSE_COUNT_SETTING.get(state.getMetaData().settings());
        List<String> templatesOverLimit = new ArrayList<>();
        state.getMetaData().getTemplates().forEach((templateCursor) -> {
            AtomicInteger maxFields = new AtomicInteger(0);
            String templateName = templateCursor.key;
            boolean defaultFieldSet = templateCursor.value.getSettings().get(IndexSettings.DEFAULT_FIELD_SETTING_KEY) != null;
            templateCursor.value.getMappings().forEach((mappingCursor) -> {
                String mappingTypeName = mappingCursor.key;
                MappingMetaData mappingMetaData = null;
                try {
                    mappingMetaData = new MappingMetaData(mappingCursor.value);
                } catch (IOException e) {
                    logger.error("failed to parse mapping for type {}: {}", mappingTypeName, e);
                }
                if (mappingMetaData != null && defaultFieldSet == false) {
                    maxFields.set(IndexDeprecationChecks.countFieldsRecursively(mappingMetaData.type(), mappingMetaData.sourceAsMap()));
                }
                if (maxFields.get() > maxClauseCount) {
                    templatesOverLimit.add(templateName);
                }
            });
        });

        if (templatesOverLimit.isEmpty() == false) {
            return new DeprecationIssue(DeprecationIssue.Level.WARNING,
                "Fields in index template exceed automatic field expansion limit",
                "https://www.elastic.co/guide/en/elasticsearch/reference/7.0/breaking-changes-7.0.html" +
                    "#_limiting_the_number_of_auto_expanded_fields",
                "Index templates " + templatesOverLimit + " have a number of fields which exceeds the automatic field expansion " +
                    "limit of [" + maxClauseCount + "] and does not have [" + IndexSettings.DEFAULT_FIELD_SETTING_KEY + "] set, which " +
                    "may cause queries which use automatic field expansion, such as query_string, simple_query_string, and multi_match " +
                    "to fail if fields are not explicitly specified in the query.");
        }
        return null;
    }
}
