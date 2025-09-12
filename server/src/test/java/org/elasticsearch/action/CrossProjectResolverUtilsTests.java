/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;

public class CrossProjectResolverUtilsTests extends ESTestCase {
    private RemoteClusterAwareTest remoteClusterAware = new RemoteClusterAwareTest();

    public void testFlatOnlyRewriteCrossProjectResolvableRequest() {
        AuthorizedProjectsSupplier.AuthorizedProjects authorizedProjects = new AuthorizedProjectsSupplier.AuthorizedProjects(
            "_origin",
            List.of("P1", "P2", "P3")
        );
        CrossProjectReplaceableTest crossProjectRequest = new CrossProjectReplaceableTest(
            new String[] { "logs*", "metrics*" },
            IndicesOptions.DEFAULT
        );

        CrossProjectResolverUtils.maybeRewriteCrossProjectResolvableRequest(remoteClusterAware, authorizedProjects, crossProjectRequest);

        assertThat(crossProjectRequest.getCanonicalExpressions().keySet(), containsInAnyOrder("logs*", "metrics*"));
        assertThat(
            crossProjectRequest.getCanonicalExpressions().get("logs*"),
            containsInAnyOrder("logs*", "P1:logs*", "P2:logs*", "P3:logs*")
        );
        assertThat(
            crossProjectRequest.getCanonicalExpressions().get("metrics*"),
            containsInAnyOrder("metrics*", "P1:metrics*", "P2:metrics*", "P3:metrics*")
        );
    }

    public void testFlatAndQualifiedProjectRewrite() {
        AuthorizedProjectsSupplier.AuthorizedProjects authorizedProjects = new AuthorizedProjectsSupplier.AuthorizedProjects(
            "_origin",
            List.of("P1", "P2", "P3")
        );
        CrossProjectReplaceableTest crossProjectRequest = new CrossProjectReplaceableTest(
            new String[] { "P1:logs*", "metrics*" },
            IndicesOptions.DEFAULT
        );

        CrossProjectResolverUtils.maybeRewriteCrossProjectResolvableRequest(remoteClusterAware, authorizedProjects, crossProjectRequest);

        assertThat(crossProjectRequest.getCanonicalExpressions().keySet(), containsInAnyOrder("P1:logs*", "metrics*"));
        assertThat(crossProjectRequest.getCanonicalExpressions().get("P1:logs*"), containsInAnyOrder("P1:logs*"));
        assertThat(
            crossProjectRequest.getCanonicalExpressions().get("metrics*"),
            containsInAnyOrder("metrics*", "P1:metrics*", "P2:metrics*", "P3:metrics*")
        );
    }

    public void testQualifiedOnlyLinkedProjectsRewrite() {
        AuthorizedProjectsSupplier.AuthorizedProjects authorizedProjects = new AuthorizedProjectsSupplier.AuthorizedProjects(
            "_origin",
            List.of("P1", "P2", "P3")
        );
        CrossProjectReplaceableTest crossProjectRequest = new CrossProjectReplaceableTest(
            new String[] { "P1:logs*", "P2:metrics*" },
            IndicesOptions.DEFAULT
        );

        CrossProjectResolverUtils.maybeRewriteCrossProjectResolvableRequest(remoteClusterAware, authorizedProjects, crossProjectRequest);

        assertThat(crossProjectRequest.getCanonicalExpressions().keySet(), containsInAnyOrder("P1:logs*", "P2:metrics*"));
        assertThat(crossProjectRequest.getCanonicalExpressions().get("P1:logs*"), containsInAnyOrder("P1:logs*"));
        assertThat(crossProjectRequest.getCanonicalExpressions().get("P2:metrics*"), containsInAnyOrder("P2:metrics*"));
    }

    public void testQualifiedOnlyOriginProjectsRewrite() {
        AuthorizedProjectsSupplier.AuthorizedProjects authorizedProjects = new AuthorizedProjectsSupplier.AuthorizedProjects(
            "_origin",
            List.of("P1", "P2", "P3")
        );

        CrossProjectReplaceableTest crossProjectRequest = new CrossProjectReplaceableTest(
            new String[] { "_origin:logs*", "_origin:metrics*" },
            IndicesOptions.DEFAULT
        );

        CrossProjectResolverUtils.maybeRewriteCrossProjectResolvableRequest(remoteClusterAware, authorizedProjects, crossProjectRequest);

        assertThat(crossProjectRequest.getCanonicalExpressions().keySet(), containsInAnyOrder("_origin:logs*", "_origin:metrics*"));
        assertThat(crossProjectRequest.getCanonicalExpressions().get("_origin:logs*"), containsInAnyOrder("logs*"));
        assertThat(crossProjectRequest.getCanonicalExpressions().get("_origin:metrics*"), containsInAnyOrder("metrics*"));
    }

    public void testQualifiedOriginAndLikedProjectsRewrite() {
        AuthorizedProjectsSupplier.AuthorizedProjects authorizedProjects = new AuthorizedProjectsSupplier.AuthorizedProjects(
            "_origin",
            List.of("P1", "P2", "P3")
        );
        CrossProjectReplaceableTest crossProjectRequest = new CrossProjectReplaceableTest(
            new String[] { "P1:logs*", "_origin:metrics*" },
            IndicesOptions.DEFAULT
        );

        CrossProjectResolverUtils.maybeRewriteCrossProjectResolvableRequest(remoteClusterAware, authorizedProjects, crossProjectRequest);

        assertThat(crossProjectRequest.getCanonicalExpressions().keySet(), containsInAnyOrder("P1:logs*", "_origin:metrics*"));
        assertThat(crossProjectRequest.getCanonicalExpressions().get("P1:logs*"), containsInAnyOrder("P1:logs*"));
        assertThat(crossProjectRequest.getCanonicalExpressions().get("_origin:metrics*"), containsInAnyOrder("metrics*"));
    }

    public void testQualifiedStartsWithProjectWildcardRewrite() {
        AuthorizedProjectsSupplier.AuthorizedProjects authorizedProjects = new AuthorizedProjectsSupplier.AuthorizedProjects(
            "_origin",
            List.of("P1", "P2", "Q1", "Q2")
        );
        CrossProjectReplaceableTest crossProjectRequest = new CrossProjectReplaceableTest(
            new String[] { "P*:metrics*" },
            IndicesOptions.DEFAULT
        );

        CrossProjectResolverUtils.maybeRewriteCrossProjectResolvableRequest(remoteClusterAware, authorizedProjects, crossProjectRequest);

        assertThat(crossProjectRequest.getCanonicalExpressions().keySet(), containsInAnyOrder("P*:metrics*"));
        assertThat(crossProjectRequest.getCanonicalExpressions().get("P*:metrics*"), containsInAnyOrder("P1:metrics*", "P2:metrics*"));
    }

    public void testQualifiedEndsWithProjectWildcardRewrite() {
        AuthorizedProjectsSupplier.AuthorizedProjects authorizedProjects = new AuthorizedProjectsSupplier.AuthorizedProjects(
            "_origin",
            List.of("P1", "P2", "Q1", "Q2")
        );
        CrossProjectReplaceableTest crossProjectRequest = new CrossProjectReplaceableTest(
            new String[] { "*1:metrics*" },
            IndicesOptions.DEFAULT
        );

        CrossProjectResolverUtils.maybeRewriteCrossProjectResolvableRequest(remoteClusterAware, authorizedProjects, crossProjectRequest);

        assertThat(crossProjectRequest.getCanonicalExpressions().keySet(), containsInAnyOrder("*1:metrics*"));
        assertThat(crossProjectRequest.getCanonicalExpressions().get("*1:metrics*"), containsInAnyOrder("P1:metrics*", "Q1:metrics*"));
    }

    public void testProjectWildcardNotMatchingAnythingShouldThrow() {
        AuthorizedProjectsSupplier.AuthorizedProjects authorizedProjects = new AuthorizedProjectsSupplier.AuthorizedProjects(
            "_origin",
            List.of("P1", "P2", "Q1", "Q2")
        );
        CrossProjectReplaceableTest crossProjectRequest = new CrossProjectReplaceableTest(
            new String[] { "S*:metrics*" },
            IndicesOptions.DEFAULT
        );

        // In this case we are throwing because no resource was found. Do we want to throw or should we continue with a list of empty
        // "canonical" indices and perhaps throw later on based on IndicesOptions?
        expectThrows(
            ResourceNotFoundException.class,
            () -> CrossProjectResolverUtils.maybeRewriteCrossProjectResolvableRequest(
                remoteClusterAware,
                authorizedProjects,
                crossProjectRequest
            )
        );
    }

    public void testRewritingShouldThrowOnIndexExclusions() {
        // TODO Implement index exclusion.
        AuthorizedProjectsSupplier.AuthorizedProjects authorizedProjects = new AuthorizedProjectsSupplier.AuthorizedProjects(
            "_origin",
            List.of("P1", "P2", "Q1", "Q2")
        );
        CrossProjectReplaceableTest crossProjectRequest = new CrossProjectReplaceableTest(
            new String[] { "P*:metrics*", "-P1:metrics*" },
            IndicesOptions.DEFAULT
        );

        // In this case we are throwing because no resource was found. Do we want to throw or should we continue with a list of empty
        // "canonical" indices and perhaps throw later on based on IndicesOptions?
        expectThrows(
            IllegalArgumentException.class,
            () -> CrossProjectResolverUtils.maybeRewriteCrossProjectResolvableRequest(
                remoteClusterAware,
                authorizedProjects,
                crossProjectRequest
            )
        );
    }

    public void testWildcardOnlyProjectRewrite() {
        AuthorizedProjectsSupplier.AuthorizedProjects authorizedProjects = new AuthorizedProjectsSupplier.AuthorizedProjects(
            "_origin",
            List.of("P1", "P2", "Q1", "Q2")
        );
        CrossProjectReplaceableTest crossProjectRequest = new CrossProjectReplaceableTest(
            new String[] { "*:metrics*" },
            IndicesOptions.DEFAULT
        );

        CrossProjectResolverUtils.maybeRewriteCrossProjectResolvableRequest(remoteClusterAware, authorizedProjects, crossProjectRequest);

        assertThat(crossProjectRequest.getCanonicalExpressions().keySet(), containsInAnyOrder("*:metrics*"));
        assertThat(
            crossProjectRequest.getCanonicalExpressions().get("*:metrics*"),
            containsInAnyOrder("P1:metrics*", "P2:metrics*", "Q1:metrics*", "Q2:metrics*", "metrics*")
        );
    }

    public void testEmptyExpressionShouldMatchAll() {
        AuthorizedProjectsSupplier.AuthorizedProjects authorizedProjects = new AuthorizedProjectsSupplier.AuthorizedProjects(
            "_origin",
            List.of("P1", "P2")
        );
        CrossProjectReplaceableTest crossProjectRequest = new CrossProjectReplaceableTest(new String[] {}, IndicesOptions.DEFAULT);

        CrossProjectResolverUtils.maybeRewriteCrossProjectResolvableRequest(remoteClusterAware, authorizedProjects, crossProjectRequest);

        assertThat(crossProjectRequest.getCanonicalExpressions().keySet(), containsInAnyOrder("*"));
        assertThat(crossProjectRequest.getCanonicalExpressions().get("*"), containsInAnyOrder("P1:*", "P2:*", "*"));
    }

    public void testWildcardExpressionShouldMatchAll() {
        AuthorizedProjectsSupplier.AuthorizedProjects authorizedProjects = new AuthorizedProjectsSupplier.AuthorizedProjects(
            "_origin",
            List.of("P1", "P2")
        );
        CrossProjectReplaceableTest crossProjectRequest = new CrossProjectReplaceableTest(new String[] { "*" }, IndicesOptions.DEFAULT);

        CrossProjectResolverUtils.maybeRewriteCrossProjectResolvableRequest(remoteClusterAware, authorizedProjects, crossProjectRequest);

        assertThat(crossProjectRequest.getCanonicalExpressions().keySet(), containsInAnyOrder("*"));
        assertThat(crossProjectRequest.getCanonicalExpressions().get("*"), containsInAnyOrder("P1:*", "P2:*", "*"));
    }

    public void test_ALLExpressionShouldMatchAll() {
        AuthorizedProjectsSupplier.AuthorizedProjects authorizedProjects = new AuthorizedProjectsSupplier.AuthorizedProjects(
            "_origin",
            List.of("P1", "P2")
        );
        String all = randomBoolean() ? "_ALL" : "_all";
        CrossProjectReplaceableTest crossProjectRequest = new CrossProjectReplaceableTest(new String[] { all }, IndicesOptions.DEFAULT);

        CrossProjectResolverUtils.maybeRewriteCrossProjectResolvableRequest(remoteClusterAware, authorizedProjects, crossProjectRequest);

        assertThat(crossProjectRequest.getCanonicalExpressions().keySet(), containsInAnyOrder("*"));
        assertThat(crossProjectRequest.getCanonicalExpressions().get("*"), containsInAnyOrder("P1:*", "P2:*", "*"));
    }

    private static class RemoteClusterAwareTest extends RemoteClusterAware {
        RemoteClusterAwareTest() {
            super(Settings.EMPTY);
        }

        @Override
        protected void updateRemoteCluster(String clusterAlias, Settings settings) {

        }
    }

    private static class CrossProjectReplaceableTest implements IndicesRequest.CrossProjectReplaceable {
        private String[] indices;
        private IndicesOptions options;
        private Map<String, List<String>> canonicalExpressions;

        CrossProjectReplaceableTest(String[] indices, IndicesOptions options) {
            this.indices = indices;
            this.options = options;
        }

        @Override
        public IndicesRequest indices(String... indices) {
            this.indices = indices;
            return this;
        }

        @Override
        public String[] indices() {
            return indices;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return options;
        }

        @Override
        public void setCanonicalExpressions(Map<String, List<String>> canonicalExpressions) {
            this.canonicalExpressions = canonicalExpressions;
        }

        @Override
        public Map<String, List<String>> getCanonicalExpressions() {
            // Maybe this should be a record that contains also if the expression was flat or not.
            return canonicalExpressions;
        }
    }
}
