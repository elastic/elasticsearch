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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authz.permission.ResourcePrivileges;
import org.elasticsearch.xpack.esql.into.IntoDestinationAuthorizer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * {@code INTO} reports things about its destination while planning, from cluster state that security does not filter. The session
 * has to authorize the destination first, so that a caller who may not write cannot use those reports to probe it.
 */
public class EsqlSessionIntoAuthorizationTests extends ESTestCase {
    private static final String EXISTING = "existing";

    private final List<String> asked = new ArrayList<>();

    private IntoDestinationAuthorizer authorizer(boolean grant) {
        return new IntoDestinationAuthorizer(() -> "alice", (request, listener) -> {
            var privileges = request.indexPrivileges()[0];
            asked.add(privileges.getIndices()[0]);
            ResourcePrivileges.Builder resource = ResourcePrivileges.builder(privileges.getIndices()[0]);
            for (String privilege : privileges.getPrivileges()) {
                resource.addPrivilege(privilege, grant);
            }
            listener.onResponse(new HasPrivilegesResponse("alice", grant, Map.of(), List.of(resource.build()), Map.of()));
        });
    }

    private static ProjectMetadata project() {
        IndexMetadata index = IndexMetadata.builder(EXISTING)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        return ProjectMetadata.builder(randomProjectIdOrDefault()).put(index, false).build();
    }

    private Exception authorize(IntoDestinationAuthorizer authorizer, String query) throws Exception {
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        EsqlSession.authorizeIntoDestination(authorizer, TEST_PARSER.parseQuery(query), project(), future);
        try {
            future.get();
            return null;
        } catch (ExecutionException e) {
            return (Exception) e.getCause();
        }
    }

    public void testQueryWithoutIntoAsksForNothing() throws Exception {
        assertNull(authorize(authorizer(false), "ROW x = 1"));
        assertThat(asked, empty());
    }

    public void testAuthorizedDestinationPasses() throws Exception {
        assertNull(authorize(authorizer(true), "ROW x = 1 | INTO " + EXISTING));
        assertThat(asked, contains(EXISTING));
    }

    public void testDeniedDestinationIsForbidden() throws Exception {
        Exception e = authorize(authorizer(false), "ROW x = 1 | INTO " + EXISTING);
        assertThat(e, instanceOf(ElasticsearchSecurityException.class));
    }

    /** The probe of the issue: a ROW needs no index privilege, so only the destination can stop the caller. */
    public void testDenialDoesNotDependOnWhetherTheDestinationExists() throws Exception {
        Exception existing = authorize(authorizer(false), "ROW x = 1 | INTO " + EXISTING);
        Exception missing = authorize(authorizer(false), "ROW x = 1 | INTO missing");
        assertThat(existing.getMessage().replace(EXISTING, "<dest>"), equalTo(missing.getMessage().replace("missing", "<dest>")));
    }

    public void testDestinationIsAuthorizedByItsResolvedName() throws Exception {
        assertNull(authorize(authorizer(true), "ROW x = 1 | INTO <logs-{now/d}>"));
        assertThat(asked.size(), equalTo(1));
        assertFalse(asked.get(0).contains("{"));
    }
}
