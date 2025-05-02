/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.multiproject.MultiProjectRestTestCase;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class ClusterStatsMultiProjectIT extends MultiProjectRestTestCase {

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

    public void testStatsAcrossMultipleProjects() throws Exception {
        var projectId1 = randomIdentifier();
        var projectId2 = randomIdentifier();
        var projectId3 = randomIdentifier();

        createProject(projectId1);
        createProject(projectId2);
        createProject(projectId3);

        createIndex(projectId1, "i1");
        createIndex(projectId1, "i2");
        createIndex(projectId1, "i3");

        createIndex(projectId2, "i1");
        createIndex(projectId2, "i2");

        createIndex(projectId3, "idx");

        for (int i = 0; i < 5; i++) {
            createDocument(projectId1, "i1", "{ \"proj\":1, \"id\":" + i + ", \"b1\":true, \"b2\":false }");
        }
        refreshIndex(projectId1, "i1");
        for (int i = 0; i < 3; i++) {
            createDocument(projectId3, "idx", "{ \"proj\":3, \"date\":\"2020-02-20T20:20:20\" }");
        }
        refreshIndex(projectId3, "idx");

        ObjectPath response = new ObjectPath(getAsMap("/_cluster/stats"));
        assertThat(response.evaluate("status"), equalTo("green"));
        assertThat(response.evaluate("indices.count"), equalTo(3 + 2 + 1));
        assertThat(response.evaluate("indices.docs.count"), equalTo(5 + 3));
        assertThat(response.evaluate("indices.mappings.total_field_count"), equalTo(4 + 2));

        final List<Map<String, Object>> fieldTypes = response.evaluate("indices.mappings.field_types");
        assertThat(fieldTypes.size(), equalTo(3));
        fieldTypes.sort(Comparator.comparing(o -> String.valueOf(o.get("name"))));

        assertThat(fieldTypes.get(0).get("name"), equalTo("boolean"));
        assertThat(fieldTypes.get(0).get("count"), equalTo(2));
        assertThat(fieldTypes.get(0).get("index_count"), equalTo(1));

        assertThat(fieldTypes.get(1).get("name"), equalTo("date"));
        assertThat(fieldTypes.get(1).get("count"), equalTo(1));
        assertThat(fieldTypes.get(1).get("index_count"), equalTo(1));

        assertThat(fieldTypes.get(2).get("name"), equalTo("long"));
        assertThat(fieldTypes.get(2).get("count"), equalTo(3));
        assertThat(fieldTypes.get(2).get("index_count"), equalTo(2));
    }

    private void createIndex(String projectId, String indexName) throws IOException {
        createIndex(req -> {
            setRequestProjectId(req, projectId);
            return client().performRequest(req);
        }, indexName, null, null, null);
    }

    private void createDocument(String projectId, String indexName, String body) throws IOException {
        Request request = new Request("POST", "/" + indexName + "/_doc");
        request.setJsonEntity(body);
        setRequestProjectId(request, projectId);
        client().performRequest(request);
    }

    private static Response refreshIndex(String projectId, String indexName) throws IOException {
        return client().performRequest(setRequestProjectId(new Request("POST", "/" + indexName + "/_refresh"), projectId));
    }

}
