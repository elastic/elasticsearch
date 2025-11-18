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
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.multiproject.MultiProjectRestTestCase;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.After;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Multi-project integration tests for the parts of the _nodes/stats and _info APIs which return project-scoped stats.
 */
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

    @After
    public void deletePipelines() throws IOException {
        for (String projectId : getProjectIds(adminClient())) {
            for (String pipelineId : getPipelineIds(projectId)) {
                client().performRequest(setRequestProjectId(new Request("DELETE", "/_ingest/pipeline/" + pipelineId), projectId));
            }
        }
    }

    private static Set<String> getPipelineIds(String projectId) throws IOException {
        try {
            return responseAsMap(client().performRequest(setRequestProjectId(new Request("GET", "/_ingest/pipeline"), projectId))).keySet();
        } catch (ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(404)); // expected if there are no pipelines
            return Set.of();
        }
    }

    public void testIndicesStats() throws IOException {
        // Create two projects. We will use the default project as the third project in this test.
        createProject("project-1");
        createProject("project-2");

        // Create and write data into a number of indices.
        // Some of the index names are used only in one project, some in two projects, some in all three.
        int numDocs1Only = createPopulatedIndex("project-1", "my-index-project-1-only");
        int numDocs2Only = createPopulatedIndex("project-2", "my-index-project-2-only");
        int numDocsDefaultOnly = createPopulatedIndex("default", "my-index-default-project-only");
        int numDocs1Of1And2 = createPopulatedIndex("project-1", "my-index-projects-1-and-2");
        int numDocs2Of1And2 = createPopulatedIndex("project-2", "my-index-projects-1-and-2");
        int numDocs2Of2AndDefault = createPopulatedIndex("project-2", "my-index-projects-2-and-default");
        int numDocsDefaultOf2AndDefault = createPopulatedIndex("default", "my-index-projects-2-and-default");
        int numDocs1All = createPopulatedIndex("project-1", "my-index-all-projects");
        int numDocs2All = createPopulatedIndex("project-2", "my-index-all-projects");
        int numDocsDefaultAll = createPopulatedIndex("default", "my-index-all-projects");

        String nodeId = findNodeId();

        // Get indices stats at node level, and assert that the total docs count is correct:
        int totalCount = ObjectPath.evaluate(getAsMap("/_nodes/stats/indices?level=node"), "nodes." + nodeId + ".indices.docs.count");
        assertThat(
            totalCount,
            equalTo(
                numDocs1Only + numDocs2Only + numDocsDefaultOnly + numDocs1Of1And2 + numDocs2Of1And2 + numDocs2Of2AndDefault
                    + numDocsDefaultOf2AndDefault + numDocs1All + numDocs2All + numDocsDefaultAll
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
            "default/my-index-default-project-only",
            "project-1/my-index-projects-1-and-2",
            "project-2/my-index-projects-1-and-2",
            "project-2/my-index-projects-2-and-default",
            "default/my-index-projects-2-and-default",
            "project-1/my-index-all-projects",
            "project-2/my-index-all-projects",
            "default/my-index-all-projects"
        );
        assertThat(indexStats.keySet(), equalTo(expectedProjectIdAndIndexNames));
        // ...and assert that the docs counts are correct for some of the indices:
        assertThat(ObjectPath.evaluate(indexStats, "project-1/my-index-project-1-only.docs.count"), equalTo(numDocs1Only));
        assertThat(ObjectPath.evaluate(indexStats, "project-2/my-index-projects-2-and-default.docs.count"), equalTo(numDocs2Of2AndDefault));
        assertThat(ObjectPath.evaluate(indexStats, "default/my-index-all-projects.docs.count"), equalTo(numDocsDefaultAll));

        // Get indices stats at shard level...
        Map<String, Object> shardStats = ObjectPath.evaluate(
            getAsMap("/_nodes/stats/indices?level=shards"),
            "nodes." + nodeId + ".indices.shards"
        );
        // ...assert that all the indices are present, prefixed by their project IDs...
        assertThat(shardStats.keySet(), equalTo(expectedProjectIdAndIndexNames));
        // ...assert that the entry for the first index has exactly one entry (for its single shard)...
        List<Map<String, Object>> index1ShardStats = ObjectPath.evaluate(shardStats, "project-1/my-index-project-1-only");
        assertThat(index1ShardStats, hasSize(1));
        // ...and assert that that is shard 0 and that the doc count is correct:
        assertThat(ObjectPath.evaluate(index1ShardStats.getFirst(), "0.docs.count"), equalTo(numDocs1Only));
    }

    // Warning: Some ingest stats are not reset as part of test tear-down. This only works because we do all the ingest tests in one method.
    public void testIngestStats() throws IOException {
        // Create two projects. We will use the default project as the third project in this test.
        createProject("project-1");
        createProject("project-2");

        // Create and run data through a number of indices.
        // Some of the pipeline names are used only in one project, some in two projects, some in all three.
        int numDocs1Only = createAndUsePipeline("project-1", "my-pipeline-project-1-only");
        int numDocs2Only = createAndUsePipeline("project-2", "my-pipeline-project-2-only");
        int numDocsDefaultOnly = createAndUsePipeline("default", "my-pipeline-default-project-only");
        int numDocs1Of1And2 = createAndUsePipeline("project-1", "my-pipeline-projects-1-and-2");
        int numDocs2Of1And2 = createAndUsePipeline("project-2", "my-pipeline-projects-1-and-2");
        int numDocs2Of2AndDefault = createAndUsePipeline("project-2", "my-pipeline-projects-2-and-default");
        int numDocsDefaultOf2AndDefault = createAndUsePipeline("default", "my-pipeline-projects-2-and-default");
        int numDocs1All = createAndUsePipeline("project-1", "my-pipeline-all-projects");
        int numDocs2All = createAndUsePipeline("project-2", "my-pipeline-all-projects");
        int numDocsDefaultAll = createAndUsePipeline("default", "my-pipeline-all-projects");

        // Get the ingest stats from _nodes/stats and assert they are correct:
        Map<String, Object> ingestNodesStats = ObjectPath.evaluate(getAsMap("/_nodes/stats/ingest"), "nodes." + findNodeId() + ".ingest");
        assertIngestStats(
            ingestNodesStats,
            numDocs1Only,
            numDocs2Only,
            numDocsDefaultOnly,
            numDocs1Of1And2,
            numDocs2Of1And2,
            numDocs2Of2AndDefault,
            numDocsDefaultOf2AndDefault,
            numDocs1All,
            numDocs2All,
            numDocsDefaultAll
        );
        // Do the same thing for the ingest stats from _info:
        Map<String, Object> ingestInfo = ObjectPath.evaluate(getAsMap("/_info/ingest"), "ingest");
        assertIngestStats(
            ingestInfo,
            numDocs1Only,
            numDocs2Only,
            numDocsDefaultOnly,
            numDocs1Of1And2,
            numDocs2Of1And2,
            numDocs2Of2AndDefault,
            numDocsDefaultOf2AndDefault,
            numDocs1All,
            numDocs2All,
            numDocsDefaultAll
        );
    }

    private static void assertIngestStats(
        Map<String, Object> ingestNodesStats,
        int numDocs1Only,
        int numDocs2Only,
        int numDocsDefaultOnly,
        int numDocs1Of1And2,
        int numDocs2Of1And2,
        int numDocs2Of2AndDefault,
        int numDocsDefaultOf2AndDefault,
        int numDocs1All,
        int numDocs2All,
        int numDocsDefaultAll
    ) throws IOException {
        // Assert that the total count is correct...
        assertThat(
            ObjectPath.evaluate(ingestNodesStats, "total.count"),
            equalTo(
                numDocs1Only + numDocs2Only + numDocsDefaultOnly + numDocs1Of1And2 + numDocs2Of1And2 + numDocs2Of2AndDefault
                    + numDocsDefaultOf2AndDefault + numDocs1All + numDocs2All + numDocsDefaultAll
            )
        );
        // ...assert that all the pipelines are present, prefixed by their project IDs...
        Map<String, Object> pipelineStats = ObjectPath.evaluate(ingestNodesStats, "pipelines");
        assertThat(
            pipelineStats.keySet(),
            containsInAnyOrder(
                "project-1/my-pipeline-project-1-only",
                "project-2/my-pipeline-project-2-only",
                "default/my-pipeline-default-project-only",
                "project-1/my-pipeline-projects-1-and-2",
                "project-2/my-pipeline-projects-1-and-2",
                "project-2/my-pipeline-projects-2-and-default",
                "default/my-pipeline-projects-2-and-default",
                "project-1/my-pipeline-all-projects",
                "project-2/my-pipeline-all-projects",
                "default/my-pipeline-all-projects"
            )
        );
        // ...assert that the pipeline doc counts are for some of the pipelines correct...
        assertThat(ObjectPath.evaluate(pipelineStats, "project-1/my-pipeline-project-1-only.count"), equalTo(numDocs1Only));
        assertThat(
            ObjectPath.evaluate(pipelineStats, "project-2/my-pipeline-projects-2-and-default.count"),
            equalTo(numDocs2Of2AndDefault)
        );
        assertThat(ObjectPath.evaluate(pipelineStats, "default/my-pipeline-all-projects.count"), equalTo(numDocsDefaultAll));
        // ...and that the processors are correct with the correct counts:
        // (the counts for the lowercase processors should be halved because it has an if condition which triggers half the time)
        Map<String, Object> processorStatsPipeline1Only = extractMergedProcessorStats(
            pipelineStats,
            "project-1/my-pipeline-project-1-only"
        );
        assertThat(ObjectPath.evaluate(processorStatsPipeline1Only, "set.stats.count"), equalTo(numDocs1Only));
        assertThat(ObjectPath.evaluate(processorStatsPipeline1Only, "lowercase.stats.count"), equalTo(numDocs1Only / 2));
        Map<String, Object> processorStatsPipeline2Of2AndDefault = extractMergedProcessorStats(
            pipelineStats,
            "project-2/my-pipeline-projects-2-and-default"
        );
        assertThat(ObjectPath.evaluate(processorStatsPipeline2Of2AndDefault, "set.stats.count"), equalTo(numDocs2Of2AndDefault));
        assertThat(ObjectPath.evaluate(processorStatsPipeline2Of2AndDefault, "lowercase.stats.count"), equalTo(numDocs2Of2AndDefault / 2));
        Map<String, Object> processorStatsPipelineDefaultAll = extractMergedProcessorStats(
            pipelineStats,
            "default/my-pipeline-all-projects"
        );
        assertThat(ObjectPath.evaluate(processorStatsPipelineDefaultAll, "set.stats.count"), equalTo(numDocsDefaultAll));
        assertThat(ObjectPath.evaluate(processorStatsPipelineDefaultAll, "lowercase.stats.count"), equalTo(numDocsDefaultAll / 2));
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

    /**
     * Given the map of all the stats for all the pipelines, extracts the processor stats for the given pipeline ID. The list of maps is
     * merged into a single map.
     */
    private static Map<String, Object> extractMergedProcessorStats(Map<String, Object> pipelineStats, String pipelineId)
        throws IOException {
        Map<String, Object> merged = new HashMap<>();
        ObjectPath.<List<Map<String, Object>>>evaluate(pipelineStats, pipelineId + ".processors").forEach(merged::putAll);
        return merged;
    }
}
