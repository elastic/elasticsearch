/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class ClusterDeprecationChecks {

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
                    .anyMatch(processorConfig ->
                        false == ConfigurationUtils.readBooleanProperty(null, null, processorConfig, "ecs", false));
            })
            .map(PipelineConfiguration::getId)
            .sorted() // Make the warning consistent for testing purposes
            .collect(Collectors.toList());
        if (pipelinesWithDeprecatedEcsConfig.isEmpty() == false) {
            return new DeprecationIssue(DeprecationIssue.Level.WARNING,
                "User-Agent ingest plugin will use ECS-formatted output",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                    "#ingest-user-agent-ecs-always",
                "Ingest pipelines " + pipelinesWithDeprecatedEcsConfig + " will change to using ECS output format in 7.0");
        }
        return null;

    }
}
