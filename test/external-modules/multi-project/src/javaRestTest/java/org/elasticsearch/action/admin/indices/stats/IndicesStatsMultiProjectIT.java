/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.stats;

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
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class IndicesStatsMultiProjectIT extends MultiProjectRestTestCase {

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

        // Check indices stats for project 1.
        ObjectPath statsForProject1 = getAsObjectPathInProject("/_stats", "project-1");
        assertThat(statsForProject1.evaluate("_all.total.docs.count"), equalTo(numDocs1Only + numDocs1Of1And2 + numDocs1All));
        assertThat(
            statsForProject1.<Map<String, Object>>evaluate("indices").keySet(),
            containsInAnyOrder("my-index-project-1-only", "my-index-projects-1-and-2", "my-index-all-projects")
        );
        assertThat(statsForProject1.evaluate("indices.my-index-project-1-only.total.docs.count"), equalTo(numDocs1Only));
        assertThat(statsForProject1.evaluate("indices.my-index-projects-1-and-2.total.docs.count"), equalTo(numDocs1Of1And2));
        assertThat(statsForProject1.evaluate("indices.my-index-all-projects.total.docs.count"), equalTo(numDocs1All));

        // Check indices stats for project 2.
        ObjectPath statsForProject2 = getAsObjectPathInProject("/_stats", "project-2");
        assertThat(
            statsForProject2.evaluate("_all.total.docs.count"),
            equalTo(numDocs2Only + numDocs2Of1And2 + numDocs2Of2AndDefault + numDocs2All)
        );
        assertThat(
            statsForProject2.<Map<String, Object>>evaluate("indices").keySet(),
            containsInAnyOrder(
                "my-index-project-2-only",
                "my-index-projects-1-and-2",
                "my-index-projects-2-and-default",
                "my-index-all-projects"
            )
        );
        assertThat(statsForProject2.evaluate("indices.my-index-all-projects.total.docs.count"), equalTo(numDocs2All));

        // Check indices stats for default project.
        ObjectPath statsForDefaultProject = getAsObjectPathInDefaultProject("/_stats");
        assertThat(
            statsForDefaultProject.evaluate("_all.total.docs.count"),
            equalTo(numDocsDefaultOnly + numDocsDefaultOf2AndDefault + numDocsDefaultAll)
        );
        assertThat(
            statsForDefaultProject.<Map<String, Object>>evaluate("indices").keySet(),
            containsInAnyOrder("my-index-default-project-only", "my-index-projects-2-and-default", "my-index-all-projects")
        );
        assertThat(statsForDefaultProject.evaluate("indices.my-index-all-projects.total.docs.count"), equalTo(numDocsDefaultAll));

        // Check single-index stats for each project.
        assertThat(
            getAsObjectPathInProject("/my-index-all-projects/_stats", "project-1").evaluate("_all.total.docs.count"),
            equalTo(numDocs1All)
        );
        assertThat(
            getAsObjectPathInProject("/my-index-all-projects/_stats", "project-2").evaluate("_all.total.docs.count"),
            equalTo(numDocs2All)
        );
        assertThat(
            getAsObjectPathInDefaultProject("/my-index-all-projects/_stats").evaluate("_all.total.docs.count"),
            equalTo(numDocsDefaultAll)
        );

        // Check that getting single-index stats for an index that does not exist in a project returns a 404.
        ResponseException exception = assertThrows(
            ResponseException.class,
            () -> client().performRequest(setRequestProjectId(new Request("GET", "/my-index-project-1-only/_stats"), "project-2"))
        );
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(404));

        // Check using a wildcard gets the matching indices for that project.
        ObjectPath statsWithWildcardForProject1 = getAsObjectPathInProject("/my-index-project*/_stats", "project-1");
        assertThat(statsWithWildcardForProject1.evaluate("_all.total.docs.count"), equalTo(numDocs1Only + numDocs1Of1And2));
        assertThat(
            statsWithWildcardForProject1.<Map<String, Object>>evaluate("indices").keySet(),
            containsInAnyOrder("my-index-project-1-only", "my-index-projects-1-and-2")
        );
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

    private static ObjectPath getAsObjectPathInProject(String endpoint, String projectId) throws IOException {
        return new ObjectPath(responseAsOrderedMap(client().performRequest(setRequestProjectId(new Request("GET", endpoint), projectId))));
    }

    private static ObjectPath getAsObjectPathInDefaultProject(String endpoint) throws IOException {
        return new ObjectPath(getAsMap(endpoint));
    }
}
