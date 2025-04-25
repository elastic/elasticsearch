/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.client.Request;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.multiproject.MultiProjectRestTestCase;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class NodesStatsMultiProjectIT extends MultiProjectRestTestCase {

    private static final String PASSWORD = "hunter2";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(1)
        .distribution(DistributionType.INTEG_TEST)
        .module("test-multi-project")
        .setting("test.multi_project.enabled", "true")
        .setting("xpack.security.enabled", "true")
        .user("admin", PASSWORD)
        .module("ingest-common")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        final String token = basicAuthHeaderValue("admin", new SecureString(PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testIndicesStats_returnsStatsFromAllProjectsWithProjectIdPrefix() throws IOException {
        createProject("project-1");
        createProject("project-2");
        createProject("project-3");
        // Create and write data into a number of indices.
        // Some of the index names are used only in one project, some in two projects, some in all three.
        int numDocs1only = createPopulatedIndex("project-1", "my-index-project-1-only");
        int numDocs2only = createPopulatedIndex("project-2", "my-index-project-2-only");
        int numDocs3only = createPopulatedIndex("project-3", "my-index-project-3-only");
        int numDocs1Of1And2 = createPopulatedIndex("project-1", "my-index-projects-1-and-2");
        int numDocs2Of1And2 = createPopulatedIndex("project-2", "my-index-projects-1-and-2");
        int numDocs2Of2And3 = createPopulatedIndex("project-2", "my-index-projects-2-and-3");
        int numDocs3Of2And3 = createPopulatedIndex("project-3", "my-index-projects-2-and-3");
        int numDocs1all = createPopulatedIndex("project-1", "my-index-all-projects");
        int numDocs2all = createPopulatedIndex("project-2", "my-index-all-projects");
        int numDocs3all = createPopulatedIndex("project-3", "my-index-all-projects");

        String nodeId = findNodeId();

        // Get indices stats at node level, and assert that the total docs count is correct:
        int totalCount = ObjectPath.evaluate(getAsMap("/_nodes/stats/indices?level=node"), "nodes." + nodeId + ".indices.docs.count");
        assertThat(
            totalCount,
            equalTo(
                numDocs1only + numDocs2only + numDocs3only + numDocs1Of1And2 + numDocs2Of1And2 + numDocs2Of2And3 + numDocs3Of2And3
                    + numDocs1all + numDocs2all + numDocs3all
            )
        );

        // Get indices stats at index level...
        Map<String, Object> indexStats = ObjectPath.evaluate(
            getAsMap("/_nodes/stats/indices?level=indices"),
            "nodes." + nodeId + ".indices.indices"
        );
        // ...assert that all the indices are present, prefixed by their project IDs...
        Set<String> expectedProjectIdAndIndexNames = Set.of(
            "project-1/my-index-project-1-only",
            "project-2/my-index-project-2-only",
            "project-3/my-index-project-3-only",
            "project-1/my-index-projects-1-and-2",
            "project-2/my-index-projects-1-and-2",
            "project-2/my-index-projects-2-and-3",
            "project-3/my-index-projects-2-and-3",
            "project-1/my-index-all-projects",
            "project-2/my-index-all-projects",
            "project-3/my-index-all-projects"
        );
        assertThat(indexStats.keySet(), equalTo(expectedProjectIdAndIndexNames));
        // ...and assert that the docs counts are correct:
        assertThat(ObjectPath.<Integer>evaluate(indexStats, "project-1/my-index-project-1-only.docs.count"), equalTo(numDocs1only));
        assertThat(ObjectPath.<Integer>evaluate(indexStats, "project-2/my-index-project-2-only.docs.count"), equalTo(numDocs2only));
        assertThat(ObjectPath.<Integer>evaluate(indexStats, "project-3/my-index-project-3-only.docs.count"), equalTo(numDocs3only));
        assertThat(ObjectPath.<Integer>evaluate(indexStats, "project-1/my-index-projects-1-and-2.docs.count"), equalTo(numDocs1Of1And2));
        assertThat(ObjectPath.<Integer>evaluate(indexStats, "project-2/my-index-projects-1-and-2.docs.count"), equalTo(numDocs2Of1And2));
        assertThat(ObjectPath.<Integer>evaluate(indexStats, "project-2/my-index-projects-2-and-3.docs.count"), equalTo(numDocs2Of2And3));
        assertThat(ObjectPath.<Integer>evaluate(indexStats, "project-3/my-index-projects-2-and-3.docs.count"), equalTo(numDocs3Of2And3));
        assertThat(ObjectPath.<Integer>evaluate(indexStats, "project-1/my-index-all-projects.docs.count"), equalTo(numDocs1all));
        assertThat(ObjectPath.<Integer>evaluate(indexStats, "project-2/my-index-all-projects.docs.count"), equalTo(numDocs2all));
        assertThat(ObjectPath.<Integer>evaluate(indexStats, "project-3/my-index-all-projects.docs.count"), equalTo(numDocs3all));

        // Get indices stats at shard level...
        Map<String, Object> shardStats = ObjectPath.evaluate(
            getAsMap("/_nodes/stats/indices?level=shards"),
            "nodes." + nodeId + ".indices.shards"
        );
        // ...assert that all the indices are present, prefixed by their project IDs...
        assertThat(shardStats.keySet(), equalTo(expectedProjectIdAndIndexNames));
        // ...assert that the entry for the first index has exactly one entry (for its single shards)...
        List<Map<String, Object>> index1ShardStats = ObjectPath.evaluate(shardStats, "project-1/my-index-project-1-only");
        assertThat(index1ShardStats, hasSize(1));
        // ...and assert that that is shard 0 and that the doc count is correct:
        assertThat(ObjectPath.<Integer>evaluate(index1ShardStats.getFirst(), "0.docs.count"), equalTo(numDocs1only));
    }

    public void testIndicesStats_omitsProjectIdPrefixForDefaultProject() throws IOException {
        int numDocs1 = createPopulatedIndex(Metadata.DEFAULT_PROJECT_ID.id(), "my-index-1");
        int numDocs2 = createPopulatedIndex(Metadata.DEFAULT_PROJECT_ID.id(), "my-index-2");
        int numDocs3 = createPopulatedIndex(Metadata.DEFAULT_PROJECT_ID.id(), "my-index-3");

        String nodeId = findNodeId();

        // Get indices stats at index level...
        Map<String, Object> indexStats = ObjectPath.evaluate(
            getAsMap("/_nodes/stats/indices?level=indices"),
            "nodes." + nodeId + ".indices.indices"
        );
        // ...assert that all the indices are present, *without* the project ID prefix because they are on the default project...
        Set<String> expectedProjectIdAndIndexNames = Set.of("my-index-1", "my-index-2", "my-index-3");
        assertThat(indexStats.keySet(), equalTo(expectedProjectIdAndIndexNames));
        // ...and, for the first three indices, assert that the docs counts are correct:
        assertThat(ObjectPath.<Integer>evaluate(indexStats, "my-index-1.docs.count"), equalTo(numDocs1));
        assertThat(ObjectPath.<Integer>evaluate(indexStats, "my-index-2.docs.count"), equalTo(numDocs2));
        assertThat(ObjectPath.<Integer>evaluate(indexStats, "my-index-3.docs.count"), equalTo(numDocs3));

        // Get indices stats at shard level...
        Map<String, Object> shardStats = ObjectPath.evaluate(
            getAsMap("/_nodes/stats/indices?level=shards"),
            "nodes." + nodeId + ".indices.shards"
        );
        // ...assert that all the indices are present, prefixed by their project IDs...
        assertThat(shardStats.keySet(), equalTo(expectedProjectIdAndIndexNames));
        // ...assert that the entry for the first index has exactly one entry (for its single shards)...
        List<Map<String, Object>> index1ShardStats = ObjectPath.evaluate(shardStats, "my-index-1");
        assertThat(index1ShardStats, hasSize(1));
        // ...and assert that that is shard 0 and that the doc count is correct:
        assertThat(ObjectPath.<Integer>evaluate(index1ShardStats.getFirst(), "0.docs.count"), equalTo(numDocs1));
    }

    public void testIngestStats_returnsStatsFromAllProjectsWithProjectIdPrefix() throws IOException {
        createProject("project-1");
        createProject("project-2");
        createProject("project-3");
        // Create and run data through a number of indices.
        // Some of the pipeline names are used only in one project, some in two projects, some in all three.
        int numDocs1only = createAndUsePipeline("project-1", "my-pipeline-project-1-only");
        int numDocs2only = createAndUsePipeline("project-2", "my-pipeline-project-2-only");
        int numDocs3only = createAndUsePipeline("project-3", "my-pipeline-project-3-only");
        int numDocs1Of1And2 = createAndUsePipeline("project-1", "my-pipeline-projects-1-and-2");
        int numDocs2Of1And2 = createAndUsePipeline("project-2", "my-pipeline-projects-1-and-2");
        int numDocs2Of2And3 = createAndUsePipeline("project-2", "my-pipeline-projects-2-and-3");
        int numDocs3Of2And3 = createAndUsePipeline("project-3", "my-pipeline-projects-2-and-3");
        int numDocs1all = createAndUsePipeline("project-1", "my-pipeline-all-projects");
        int numDocs2all = createAndUsePipeline("project-2", "my-pipeline-all-projects");
        int numDocs3all = createAndUsePipeline("project-3", "my-pipeline-all-projects");

        // Get ingest stats...
        Map<String, Object> ingestStats = ObjectPath.evaluate(getAsMap("/_nodes/stats/ingest"), "nodes." + findNodeId() + ".ingest");
        // ...assert that the total count is correct...
        assertThat(
            ObjectPath.evaluate(ingestStats, "total.count"),
            equalTo(
                numDocs1only + numDocs2only + numDocs3only + numDocs1Of1And2 + numDocs2Of1And2 + numDocs2Of2And3 + numDocs3Of2And3
                    + numDocs1all + numDocs2all + numDocs3all
            )
        );
        // ...assert that all the pipelines are present, prefixed by their project IDs... TODO also counts
        Map<String, Object> pipelineStats = ObjectPath.evaluate(ingestStats, "pipelines");
        assertThat(
            pipelineStats.keySet(),
            containsInAnyOrder(
                "project-1/my-pipeline-project-1-only",
                "project-2/my-pipeline-project-2-only",
                "project-3/my-pipeline-project-3-only",
                "project-1/my-pipeline-projects-1-and-2",
                "project-2/my-pipeline-projects-1-and-2",
                "project-2/my-pipeline-projects-2-and-3",
                "project-3/my-pipeline-projects-2-and-3",
                "project-1/my-pipeline-all-projects",
                "project-2/my-pipeline-all-projects",
                "project-3/my-pipeline-all-projects"
            )
        );
    }

    public void testIngestStats_omitsProjectIdPrefixForDefaultProject() throws IOException {
        int numDocs1 = createAndUsePipeline(Metadata.DEFAULT_PROJECT_ID.id(), "my-pipeline-1");
        int numDocs2 = createAndUsePipeline(Metadata.DEFAULT_PROJECT_ID.id(), "my-pipeline-2");
        int numDocs3 = createAndUsePipeline(Metadata.DEFAULT_PROJECT_ID.id(), "my-pipeline-3");

        // Get ingest stats...
        Map<String, Object> ingestStats = ObjectPath.evaluate(getAsMap("/_nodes/stats/ingest"), "nodes." + findNodeId() + ".ingest");
        // ...assert that the total count is correct...
        assertThat(ObjectPath.evaluate(ingestStats, "total.count"), equalTo(numDocs1 + numDocs2 + numDocs3));
        // ...and that the pipelines are correct with the correct counts...
        Map<String, Object> pipelineStats = ObjectPath.evaluate(ingestStats, "pipelines");
        assertThat(pipelineStats.keySet(), containsInAnyOrder("my-pipeline-1", "my-pipeline-2", "my-pipeline-3"));
        assertThat(ObjectPath.evaluate(pipelineStats, "my-pipeline-1.count"), equalTo(numDocs1));
        assertThat(ObjectPath.evaluate(pipelineStats, "my-pipeline-2.count"), equalTo(numDocs2));
        assertThat(ObjectPath.evaluate(pipelineStats, "my-pipeline-3.count"), equalTo(numDocs3));
        // ...and that the processors are correct with the correct counts:
        // (the counts for the lowercase processors should be halved because it has an if condition which triggers half the time)
        Map<String, Object> processorStatsPipeline1 = mergeMaps(ObjectPath.evaluate(pipelineStats, "my-pipeline-1.processors"));
        assertThat(ObjectPath.evaluate(processorStatsPipeline1, "set.stats.count"), equalTo(numDocs1));
        assertThat(ObjectPath.evaluate(processorStatsPipeline1, "lowercase.stats.count"), equalTo(numDocs1 / 2));
        Map<String, Object> processorStatsPipeline2 = mergeMaps(ObjectPath.evaluate(pipelineStats, "my-pipeline-2.processors"));
        assertThat(ObjectPath.evaluate(processorStatsPipeline2, "set.stats.count"), equalTo(numDocs2));
        assertThat(ObjectPath.evaluate(processorStatsPipeline2, "lowercase.stats.count"), equalTo(numDocs2 / 2));
        Map<String, Object> processorStatsPipeline3 = mergeMaps(ObjectPath.evaluate(pipelineStats, "my-pipeline-3.processors"));
        assertThat(ObjectPath.evaluate(processorStatsPipeline3, "set.stats.count"), equalTo(numDocs3));
        assertThat(ObjectPath.evaluate(processorStatsPipeline3, "lowercase.stats.count"), equalTo(numDocs3 / 2));
    }

    private int createPopulatedIndex(String projectId, String indexName) throws IOException {
        createIndex(req -> {
            setRequestProjectId(req, projectId);
            return client().performRequest(req);
        }, indexName, null, null, null);
        int numDocs = randomIntBetween(5, 10);
        for (int i = 0; i < numDocs; i++) {
            Request request = new Request("POST", "/" + indexName + "/_doc");
            request.setJsonEntity(Strings.format("{ \"num\": %d, \"str\": \"%s\" }", randomInt(), randomAlphaOfLengthBetween(5, 10)));
            setRequestProjectId(request, projectId);
            client().performRequest(request);
        }
        client().performRequest(setRequestProjectId(new Request("POST", "/" + indexName + "/_refresh"), projectId));
        return numDocs;
    }

    private int createAndUsePipeline(String projectId, String pipelineId) throws IOException {
        Request createPipelineRequest = new Request("PUT", "/_ingest/pipeline/" + pipelineId);
        setRequestProjectId(createPipelineRequest, projectId);
        createPipelineRequest.setJsonEntity("""
            {
              "processors": [
                {
                  "set": {
                    "field": "foo",
                    "value": "bar"
                  }
                },
                {
                  "lowercase": {
                    "field": "str",
                    "if": "ctx.needs_lower"
                  }
                }
              ]
            }
            """);
        client().performRequest(createPipelineRequest);

        int numDocs = randomIntBetween(2, 6) * 2;
        for (int i = 0; i < numDocs; i++) {
            Request request = new Request("POST", "/my-index/_doc?pipeline=" + pipelineId);
            boolean needsLower = (i % 2) == 0; // run the lowercase processor for every other doc
            request.setJsonEntity(
                Strings.format(
                    "{ \"num\": %d, \"str\": \"%s\", \"needs_lower\": %s }",
                    randomInt(),
                    randomAlphaOfLengthBetween(5, 10),
                    needsLower
                )
            );
            setRequestProjectId(request, projectId);
            client().performRequest(request);
        }
        client().performRequest(setRequestProjectId(new Request("POST", "/my-index/_refresh"), projectId));

        return numDocs;
    }

    private static String findNodeId() throws IOException {
        return ObjectPath.<Map<String, Object>>evaluate(getAsMap("/_nodes"), "nodes").keySet().stream().findAny().orElseThrow();
    }

    private static Map<String, Object> mergeMaps(List<Map<String, Object>> maps) {
        Map<String, Object> merged = new HashMap<>();
        maps.forEach(merged::putAll);
        return merged;
    }
}
