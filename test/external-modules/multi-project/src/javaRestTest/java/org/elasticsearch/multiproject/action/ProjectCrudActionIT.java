/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.multiproject.action;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.multiproject.MultiProjectRestTestCase;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;

public class ProjectCrudActionIT extends MultiProjectRestTestCase {

    @ClassRule
    public static ElasticsearchCluster CLUSTER = ElasticsearchCluster.local()
        .distribution(DistributionType.INTEG_TEST)
        .setting("test.multi_project.enabled", "true")
        .setting("xpack.security.enabled", "false")
        .build();

    @Override
    protected String getTestRestCluster() {
        return CLUSTER.getHttpAddresses();
    }

    public void testCreateAndDeleteProject() throws IOException {
        final var projectId = randomUniqueProjectId();
        var request = new Request("PUT", "/_project/" + projectId);

        final int numberOfRequests = between(1, 8);
        final var successCount = new AtomicInteger();
        final var errorCount = new AtomicInteger();

        runInParallel(numberOfRequests, ignore -> {
            try {
                var response = client().performRequest(request);
                assertAcknowledged(response);
                successCount.incrementAndGet();
            } catch (IOException e) {
                if (e instanceof ResponseException responseException) {
                    assertThat(responseException.getMessage(), containsString("project [" + projectId + "] already exists"));
                    errorCount.incrementAndGet();
                    return;
                }
                fail(e, "unexpected exception");
            }
        });

        assertThat(successCount.get(), equalTo(1));
        assertThat(errorCount.get(), equalTo(numberOfRequests - 1));
        assertThat(getProjectIdsFromClusterState(), hasItem(projectId.id()));

        final Response response = client().performRequest(new Request("DELETE", "/_project/" + projectId));
        assertAcknowledged(response);
        assertThat(getProjectIdsFromClusterState(), not(hasItem(projectId.id())));
    }

    private Set<String> getProjectIdsFromClusterState() throws IOException {
        final Response response = client().performRequest(new Request("GET", "/_cluster/state?multi_project=true"));
        final ObjectPath clusterState = assertOKAndCreateObjectPath(response);

        final Set<String> projectIdsFromMetadata = extractProjectIds(clusterState, "metadata.projects");
        final Set<String> projectIdsFromRoutingTable = extractProjectIds(clusterState, "routing_table.projects");

        assertThat(projectIdsFromMetadata, equalTo(projectIdsFromRoutingTable));
        return projectIdsFromMetadata;
    }

    @SuppressWarnings("unchecked")
    private Set<String> extractProjectIds(ObjectPath clusterState, String path) throws IOException {
        final int numberProjects = ((List<Object>) clusterState.evaluate(path)).size();
        return IntStream.range(0, numberProjects).mapToObj(i -> {
            try {
                return (String) clusterState.evaluate(path + "." + i + ".id");
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).collect(Collectors.toUnmodifiableSet());
    }
}
