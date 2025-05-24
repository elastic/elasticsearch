/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.repositories.reservedstate;

import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * Tests that the ReservedRepositoryAction does validation, can add and remove repositories
 */
public class ReservedRepositoryActionTests extends ESTestCase {

    private TransformState<ClusterState> processJSON(ReservedRepositoryAction action, TransformState<ClusterState> prevState, String json)
        throws Exception {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            return action.transform(action.fromXContent(parser), prevState);
        }
    }

    public void testValidation() throws Exception {
        var repositoriesService = mockRepositoriesService();

        ClusterState state = ClusterState.builder(new ClusterName("elasticsearch")).build();
        TransformState<ClusterState> prevState = new TransformState<>(state, Collections.emptySet());
        ReservedRepositoryAction action = new ReservedRepositoryAction(repositoriesService);

        String badPolicyJSON = """
            {
                "repo": {
                   "type": "inter_planetary",
                   "settings": {
                      "location": "my_backup_location"
                   }
                }
            }""";

        assertEquals(
            "[repo] repository type [inter_planetary] does not exist",
            expectThrows(RepositoryException.class, () -> processJSON(action, prevState, badPolicyJSON)).getMessage()
        );
    }

    public void testAddRepo() throws Exception {
        var repositoriesService = mockRepositoriesService();

        ClusterState state = ClusterState.builder(new ClusterName("elasticsearch")).build();
        TransformState<ClusterState> prevState = new TransformState<>(state, Collections.emptySet());
        ReservedRepositoryAction action = new ReservedRepositoryAction(repositoriesService);

        String emptyJSON = "";

        TransformState<ClusterState> updatedState = processJSON(action, prevState, emptyJSON);
        assertEquals(0, updatedState.keys().size());
        assertEquals(prevState.state(), updatedState.state());

        String settingsJSON = """
            {
                "repo": {
                   "type": "fs",
                   "settings": {
                      "location": "my_backup_location"
                   }
                },
                "repo1": {
                   "type": "fs",
                   "settings": {
                      "location": "my_backup_location_1"
                   }
                }
            }""";

        prevState = updatedState;
        updatedState = processJSON(action, prevState, settingsJSON);
        assertThat(updatedState.keys(), containsInAnyOrder("repo", "repo1"));
    }

    public void testRemoveRepo() {
        var repositoriesService = mockRepositoriesService();

        ClusterState state = ClusterState.builder(new ClusterName("elasticsearch")).build();
        TransformState<ClusterState> prevState = new TransformState<>(state, Set.of("repo1"));
        ReservedRepositoryAction action = new ReservedRepositoryAction(repositoriesService);

        String emptyJSON = "";

        // We can't really fake the internal repos in RepositoriesService, it complaining that repo1 is
        // missing is sufficient to tell that we attempted to delete that repo
        assertEquals(
            "[repo1] missing",
            expectThrows(RepositoryMissingException.class, () -> processJSON(action, prevState, emptyJSON)).getMessage()
        );
    }

    private RepositoriesService mockRepositoriesService() {
        var fsFactory = new Repository.Factory() {
            @Override
            public Repository create(ProjectId projectId, RepositoryMetadata metadata) {
                var repo = mock(Repository.class);
                doAnswer(invocation -> metadata).when(repo).getMetadata();
                return repo;
            }
        };

        ThreadPool threadPool = mock(ThreadPool.class);
        RepositoriesService repositoriesService = spy(
            new RepositoriesService(
                Settings.EMPTY,
                mock(ClusterService.class),
                Map.of(),
                Map.of("fs", fsFactory),
                threadPool,
                mock(NodeClient.class),
                null
            )
        );

        doAnswer(invocation -> {
            var request = (PutRepositoryRequest) invocation.getArguments()[0];
            if (request.type().equals("inter_planetary")) {
                throw new RepositoryException(request.name(), "repository type [" + request.type() + "] does not exist");
            }
            return null;
        }).when(repositoriesService).validateRepositoryCanBeCreated(any());

        return repositoriesService;
    }
}
