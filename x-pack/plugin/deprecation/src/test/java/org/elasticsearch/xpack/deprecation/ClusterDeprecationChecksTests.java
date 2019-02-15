/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.deprecation.DeprecationChecks.CLUSTER_SETTINGS_CHECKS;

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
}
