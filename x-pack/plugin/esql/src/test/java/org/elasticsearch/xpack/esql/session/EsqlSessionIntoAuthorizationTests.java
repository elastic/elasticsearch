/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ElasticsearchStatusException;
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
import static org.hamcrest.Matchers.equalTo;

/**
 * What {@link EsqlSession#authorizeIntoDestinations} asks the authorizer, and the plan it hands on. That the authorization comes
 * before anything is reported about the destination, and the privilege rules themselves, are tested by {@code EsqlSecurityIntoIT}
 * and {@code HasPrivilegesIntoDestinationAuthorizerTests}.
 */
public class EsqlSessionIntoAuthorizationTests extends ESTestCase {
    private static final String EXISTING = "existing";
    private static final String DATE_MATH_RESOLVED = "logs-2026.01.01";

    private final List<String> asked = new ArrayList<>();

    @Before
    public void requiresIntoCommand() {
        assumeTrue("INTO command required", EsqlCapabilities.Cap.INTO_COMMAND.isEnabled());
    }

    public void testMissingDestinationIsAnErrorForAnAuthorizedCaller() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> authorize(grantingAuthorizer(), "ROW x = 1 | INTO nothing")
        );
        assertThat(e.status(), equalTo(RestStatus.NOT_FOUND));
        assertThat(e.getMessage(), containsString("INTO destination [nothing] does not exist"));
        assertThat("the caller is authorized before being told", asked, contains("nothing"));
    }

    /** Whether a destination exists is only for a caller who may write to it. */
    public void testDeniedCallerIsNotToldThatADestinationDoesNotExist() {
        IntoDestinationAuthorizer denying = (destination, listener) -> listener.onFailure(
            new ElasticsearchSecurityException("denied", RestStatus.FORBIDDEN)
        );
        ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> authorize(denying, "ROW x = 1 | INTO nothing")
        );
        assertThat(e.status(), equalTo(RestStatus.FORBIDDEN));
    }

    /** An EXPLAIN never writes, so a destination that is not there yet is fine, but the caller is still authorized. */
    public void testExplainDoesNotNeedTheDestinationToExist() throws Exception {
        authorize(grantingAuthorizer(), "ROW x = 1 | INTO nothing", false);
        assertThat(asked, contains("nothing"));
    }

    public void testDestinationIsAuthorizedByItsResolvedNameAndTheResolvedNameReplacesTheTypedOne() throws Exception {
        // A fixed date, as now/d could roll over to the next day between the name and the query
        LogicalPlan authorized = authorize(grantingAuthorizer(), "ROW x = 1 | INTO \"<logs-{2026.01.01||/d}>\"");

        assertThat(asked, contains(DATE_MATH_RESOLVED));
        // Everything after this, on whichever node, writes to the name that was authorized
        assertThat(authorized.collect(Into.class).getFirst().destination(), equalTo(DATE_MATH_RESOLVED));
    }

    private IntoDestinationAuthorizer grantingAuthorizer() {
        return (destination, listener) -> {
            asked.add(destination);
            listener.onResponse(null);
        };
    }

    /** Authorizes the destination of the query, returning the plan that results, or throwing what the authorization failed with. */
    private static LogicalPlan authorize(IntoDestinationAuthorizer authorizer, String query) throws Exception {
        return authorize(authorizer, query, true);
    }

    private static LogicalPlan authorize(IntoDestinationAuthorizer authorizer, String query, boolean requireExisting) throws Exception {
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(index(EXISTING), false)
            .put(index(DATE_MATH_RESOLVED), false)
            .build();
        PlainActionFuture<LogicalPlan> future = new PlainActionFuture<>();
        EsqlSession.authorizeIntoDestinations(authorizer, TEST_PARSER.parseQuery(query), project, requireExisting, future);
        try {
            return future.get();
        } catch (ExecutionException e) {
            throw (Exception) e.getCause();
        }
    }

    private static IndexMetadata index(String name) {
        return IndexMetadata.builder(name)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }
}
