/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.into.IntoDestinationAuthorizer;
import org.elasticsearch.xpack.esql.plan.logical.Into;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

/**
 * What {@link EsqlSession#authorizeIntoDestinations} asks the authorizer, and the plan it hands on. That the authorization comes
 * before anything is reported about the destination, and the privilege rules themselves, are tested by {@code EsqlSecurityIntoIT}
 * and {@code HasPrivilegesIntoDestinationAuthorizerTests}.
 */
public class EsqlSessionIntoAuthorizationTests extends ESTestCase {
    private static final String EXISTING = "existing";

    private final List<String> asked = new ArrayList<>();

    @Before
    public void requiresIntoCommand() {
        assumeTrue("INTO command required", EsqlCapabilities.Cap.INTO_COMMAND.isEnabled());
    }

    public void testQueryWithoutIntoAsksForNothing() throws Exception {
        authorize(grantingAuthorizer(), "ROW x = 1");
        assertThat(asked, empty());
    }

    public void testAuthorizerIsToldWhetherTheDestinationExists() throws Exception {
        authorize(grantingAuthorizer(), "ROW x = 1 | INTO " + EXISTING);
        authorize(grantingAuthorizer(), "ROW x = 1 | INTO nothing");
        assertThat(asked, contains(EXISTING + ":true", "nothing:false"));
    }

    public void testDestinationIsAuthorizedByItsResolvedNameAndTheResolvedNameReplacesTheTypedOne() throws Exception {
        LogicalPlan authorized = authorize(grantingAuthorizer(), "ROW x = 1 | INTO <logs-{now/d}>");

        assertThat(asked.size(), equalTo(1));
        String name = asked.getFirst().substring(0, asked.getFirst().lastIndexOf(':'));
        assertThat(name, startsWith("logs-"));
        assertThat(name, not(containsString("<")));
        assertThat(name, not(containsString("{")));
        // Everything after this, on whichever node, writes to the name that was authorized
        assertThat(authorized.collect(Into.class).getFirst().destination(), equalTo(name));
    }

    public void testDenialFailsTheQuery() {
        IntoDestinationAuthorizer denying = (destination, exists, listener) -> listener.onFailure(
            new ElasticsearchSecurityException("denied", RestStatus.FORBIDDEN)
        );
        ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> authorize(denying, "ROW x = 1 | INTO " + EXISTING)
        );
        assertThat(e.status(), equalTo(RestStatus.FORBIDDEN));
    }

    private IntoDestinationAuthorizer grantingAuthorizer() {
        return (destination, exists, listener) -> {
            asked.add(destination + ":" + exists);
            listener.onResponse(null);
        };
    }

    /** Authorizes the destination of the query, returning the plan that results, or throwing what the authorization failed with. */
    private static LogicalPlan authorize(IntoDestinationAuthorizer authorizer, String query) throws Exception {
        IndexMetadata existing = IndexMetadata.builder(EXISTING)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).put(existing, false).build();
        PlainActionFuture<LogicalPlan> future = new PlainActionFuture<>();
        EsqlSession.authorizeIntoDestinations(authorizer, TEST_PARSER.parseQuery(query), project, future);
        try {
            return future.get();
        } catch (ExecutionException e) {
            throw (Exception) e.getCause();
        }
    }
}
