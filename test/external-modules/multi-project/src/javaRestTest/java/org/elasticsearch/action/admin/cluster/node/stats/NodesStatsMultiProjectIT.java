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
import java.util.List;
import java.util.Map;
import java.util.Set;

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

        String nodeId = ObjectPath.<Map<String, Object>>evaluate(getAsMap("/_nodes"), "nodes").keySet().stream().findAny().orElseThrow();

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

        String nodeId = ObjectPath.<Map<String, Object>>evaluate(getAsMap("/_nodes"), "nodes").keySet().stream().findAny().orElseThrow();

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
}
