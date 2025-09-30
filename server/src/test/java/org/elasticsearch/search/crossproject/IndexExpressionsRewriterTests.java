/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.crossproject;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;

public class IndexExpressionsRewriterTests extends ESTestCase {

    public void testFlatOnlyRewrite() {
        ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        List<ProjectRoutingInfo> linked = List.of(
            createRandomProjectWithAlias("P1"),
            createRandomProjectWithAlias("P2"),
            createRandomProjectWithAlias("P3")
        );
        String[] requestedResources = new String[] { "logs*", "metrics*" };

        Map<String, List<String>> canonical = IndexExpressionsRewriter.rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(canonical.keySet(), containsInAnyOrder("logs*", "metrics*"));
        assertThat(canonical.get("logs*"), containsInAnyOrder("logs*", "P1:logs*", "P2:logs*", "P3:logs*"));
        assertThat(canonical.get("metrics*"), containsInAnyOrder("metrics*", "P1:metrics*", "P2:metrics*", "P3:metrics*"));
    }

    public void testFlatAndQualifiedRewrite() {
        ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        List<ProjectRoutingInfo> linked = List.of(
            createRandomProjectWithAlias("P1"),
            createRandomProjectWithAlias("P2"),
            createRandomProjectWithAlias("P3")
        );
        String[] requestedResources = new String[] { "P1:logs*", "metrics*" };

        Map<String, List<String>> canonical = IndexExpressionsRewriter.rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(canonical.keySet(), containsInAnyOrder("P1:logs*", "metrics*"));
        assertThat(canonical.get("P1:logs*"), containsInAnyOrder("P1:logs*"));
        assertThat(canonical.get("metrics*"), containsInAnyOrder("metrics*", "P1:metrics*", "P2:metrics*", "P3:metrics*"));
    }

    public void testQualifiedOnlyRewrite() {
        ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        List<ProjectRoutingInfo> linked = List.of(
            createRandomProjectWithAlias("P1"),
            createRandomProjectWithAlias("P2"),
            createRandomProjectWithAlias("P3")
        );
        String[] requestedResources = new String[] { "P1:logs*", "P2:metrics*" };

        Map<String, List<String>> canonical = IndexExpressionsRewriter.rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(canonical.keySet(), containsInAnyOrder("P1:logs*", "P2:metrics*"));
        assertThat(canonical.get("P1:logs*"), containsInAnyOrder("P1:logs*"));
        assertThat(canonical.get("P2:metrics*"), containsInAnyOrder("P2:metrics*"));
    }

    public void testOriginQualifiedOnlyRewrite() {
        ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        List<ProjectRoutingInfo> linked = List.of(
            createRandomProjectWithAlias("P1"),
            createRandomProjectWithAlias("P2"),
            createRandomProjectWithAlias("P3")
        );
        String[] requestedResources = new String[] { "_origin:logs*", "_origin:metrics*" };

        Map<String, List<String>> canonical = IndexExpressionsRewriter.rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(canonical.keySet(), containsInAnyOrder("_origin:logs*", "_origin:metrics*"));
        assertThat(canonical.get("_origin:logs*"), containsInAnyOrder("logs*"));
        assertThat(canonical.get("_origin:metrics*"), containsInAnyOrder("metrics*"));
    }

    public void testOriginQualifiedOnlyRewriteWithNoLikedProjects() {
        ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        List<ProjectRoutingInfo> linked = List.of();
        String[] requestedResources = new String[] { "_origin:logs*", "_origin:metrics*" };

        Map<String, List<String>> canonical = IndexExpressionsRewriter.rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(canonical.keySet(), containsInAnyOrder("_origin:logs*", "_origin:metrics*"));
        assertThat(canonical.get("_origin:logs*"), containsInAnyOrder("logs*"));
        assertThat(canonical.get("_origin:metrics*"), containsInAnyOrder("metrics*"));
    }

    public void testOriginWithDifferentAliasQualifiedOnlyRewrite() {
        String aliasForOrigin = randomAlphaOfLength(10);
        ProjectRoutingInfo origin = createRandomProjectWithAlias(aliasForOrigin);
        List<ProjectRoutingInfo> linked = List.of(
            createRandomProjectWithAlias("P1"),
            createRandomProjectWithAlias("P2"),
            createRandomProjectWithAlias("P3")
        );
        String logIndexAlias = "logs*";
        String logResource = aliasForOrigin + ":" + logIndexAlias;
        String metricsIndexAlias = "metrics*";
        String metricResource = aliasForOrigin + ":" + metricsIndexAlias;
        String[] requestedResources = new String[] { logResource, metricResource };

        Map<String, List<String>> canonical = IndexExpressionsRewriter.rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(canonical.keySet(), containsInAnyOrder(logResource, metricResource));
        assertThat(canonical.get(logResource), containsInAnyOrder(logIndexAlias));
        assertThat(canonical.get(metricResource), containsInAnyOrder(metricsIndexAlias));
    }

    public void testQualifiedLinkedAndOriginRewrite() {
        ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        List<ProjectRoutingInfo> linked = List.of(
            createRandomProjectWithAlias("P1"),
            createRandomProjectWithAlias("P2"),
            createRandomProjectWithAlias("P3")
        );
        String[] requestedResources = new String[] { "P1:logs*", "_origin:metrics*" };

        Map<String, List<String>> canonical = IndexExpressionsRewriter.rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(canonical.keySet(), containsInAnyOrder("P1:logs*", "_origin:metrics*"));
        assertThat(canonical.get("P1:logs*"), containsInAnyOrder("P1:logs*"));
        assertThat(canonical.get("_origin:metrics*"), containsInAnyOrder("metrics*"));
    }

    public void testQualifiedStartsWithProjectWildcardRewrite() {
        ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        List<ProjectRoutingInfo> linked = List.of(
            createRandomProjectWithAlias("P1"),
            createRandomProjectWithAlias("P2"),
            createRandomProjectWithAlias("Q1"),
            createRandomProjectWithAlias("Q2")
        );
        String[] requestedResources = new String[] { "Q*:metrics*" };

        Map<String, List<String>> canonical = IndexExpressionsRewriter.rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(canonical.keySet(), containsInAnyOrder("Q*:metrics*"));
        assertThat(canonical.get("Q*:metrics*"), containsInAnyOrder("Q1:metrics*", "Q2:metrics*"));
    }

    public void testQualifiedEndsWithProjectWildcardRewrite() {
        ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        List<ProjectRoutingInfo> linked = List.of(
            createRandomProjectWithAlias("P1"),
            createRandomProjectWithAlias("P2"),
            createRandomProjectWithAlias("Q1"),
            createRandomProjectWithAlias("Q2")
        );
        String[] requestedResources = new String[] { "*1:metrics*" };

        Map<String, List<String>> canonical = IndexExpressionsRewriter.rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(canonical.keySet(), containsInAnyOrder("*1:metrics*"));
        assertThat(canonical.get("*1:metrics*"), containsInAnyOrder("P1:metrics*", "Q1:metrics*"));
    }

    public void testOriginProjectMatchingTwice() {
        ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        List<ProjectRoutingInfo> linked = List.of(createRandomProjectWithAlias("P1"), createRandomProjectWithAlias("P2"));
        String[] requestedResources = new String[] { "P0:metrics*", "_origin:metrics*" };

        Map<String, List<String>> canonical = IndexExpressionsRewriter.rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(canonical.keySet(), containsInAnyOrder("P0:metrics*", "_origin:metrics*"));
        assertThat(canonical.get("P0:metrics*"), containsInAnyOrder("metrics*"));
        assertThat(canonical.get("_origin:metrics*"), containsInAnyOrder("metrics*"));
    }

    public void testUnderscoreWildcardShouldNotMatchOrigin() {
        ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        List<ProjectRoutingInfo> linked = List.of(createRandomProjectWithAlias("_P1"), createRandomProjectWithAlias("_P2"));
        String[] requestedResources = new String[] { "_*:metrics*" };

        Map<String, List<String>> canonical = IndexExpressionsRewriter.rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(canonical.keySet(), containsInAnyOrder("_*:metrics*"));
        assertThat(canonical.get("_*:metrics*"), containsInAnyOrder("_P1:metrics*", "_P2:metrics*"));
    }

    public void testDuplicateInputShouldProduceSingleOutput() {
        ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        List<ProjectRoutingInfo> linked = List.of(
            createRandomProjectWithAlias("P1"),
            createRandomProjectWithAlias("P2"),
            createRandomProjectWithAlias("Q1"),
            createRandomProjectWithAlias("Q2")
        );
        String indexPattern = "Q*:metrics*";
        String[] requestedResources = new String[] { indexPattern, indexPattern };

        Map<String, List<String>> canonical = IndexExpressionsRewriter.rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(canonical.keySet(), containsInAnyOrder(indexPattern));
        assertThat(canonical.get(indexPattern), containsInAnyOrder("Q1:metrics*", "Q2:metrics*"));
    }

    public void testProjectWildcardNotMatchingAnythingShouldThrow() {
        ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        List<ProjectRoutingInfo> linked = List.of(
            createRandomProjectWithAlias("P1"),
            createRandomProjectWithAlias("P2"),
            createRandomProjectWithAlias("Q1"),
            createRandomProjectWithAlias("Q2")
        );
        String[] requestedResources = new String[] { "S*:metrics*" };

        expectThrows(
            ResourceNotFoundException.class,
            () -> IndexExpressionsRewriter.rewriteIndexExpressions(origin, linked, requestedResources)
        );
    }

    public void testRewritingShouldThrowOnIndexExclusions() {
        // This will fail when we implement index exclusions
        ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        List<ProjectRoutingInfo> linked = List.of(
            createRandomProjectWithAlias("P1"),
            createRandomProjectWithAlias("P2"),
            createRandomProjectWithAlias("Q1"),
            createRandomProjectWithAlias("Q2")
        );
        String[] requestedResources = new String[] { "P*:metrics*", "-P1:metrics*" };

        expectThrows(
            IllegalArgumentException.class,
            () -> IndexExpressionsRewriter.rewriteIndexExpressions(origin, linked, requestedResources)
        );
    }

    public void testRewritingShouldThrowOnIndexSelectors() {
        // This will fail when we implement index exclusions
        ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        List<ProjectRoutingInfo> linked = List.of(
            createRandomProjectWithAlias("P1"),
            createRandomProjectWithAlias("P2"),
            createRandomProjectWithAlias("Q1"),
            createRandomProjectWithAlias("Q2")
        );
        String[] requestedResources = new String[] { "index::data" };

        expectThrows(
            IllegalArgumentException.class,
            () -> IndexExpressionsRewriter.rewriteIndexExpressions(origin, linked, requestedResources)
        );
    }

    public void testWildcardOnlyProjectRewrite() {
        ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        List<ProjectRoutingInfo> linked = List.of(
            createRandomProjectWithAlias("P1"),
            createRandomProjectWithAlias("P2"),
            createRandomProjectWithAlias("Q1"),
            createRandomProjectWithAlias("Q2")
        );
        String[] requestedResources = new String[] { "*:metrics*" };

        Map<String, List<String>> canonical = IndexExpressionsRewriter.rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(canonical.keySet(), containsInAnyOrder("*:metrics*"));
        assertThat(canonical.get("*:metrics*"), containsInAnyOrder("P1:metrics*", "P2:metrics*", "Q1:metrics*", "Q2:metrics*", "metrics*"));
    }

    public void testWildcardMatchesOnlyOriginProject() {
        ProjectRoutingInfo origin = createRandomProjectWithAlias("aliasForOrigin");
        List<ProjectRoutingInfo> linked = List.of(
            createRandomProjectWithAlias("P1"),
            createRandomProjectWithAlias("P2"),
            createRandomProjectWithAlias("Q1"),
            createRandomProjectWithAlias("Q2")
        );
        String[] requestedResources = new String[] { "alias*:metrics*" };

        Map<String, List<String>> canonical = IndexExpressionsRewriter.rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(canonical.keySet(), containsInAnyOrder("alias*:metrics*"));
        assertThat(canonical.get("alias*:metrics*"), containsInAnyOrder("metrics*"));
    }

    public void testEmptyExpressionShouldMatchAll() {
        ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        List<ProjectRoutingInfo> linked = List.of(createRandomProjectWithAlias("P1"), createRandomProjectWithAlias("P2"));
        String[] requestedResources = new String[] {};

        Map<String, List<String>> canonical = IndexExpressionsRewriter.rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(canonical.keySet(), containsInAnyOrder("_all"));
        assertThat(canonical.get("_all"), containsInAnyOrder("P1:_all", "P2:_all", "_all"));
    }

    public void testNullExpressionShouldMatchAll() {
        ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        List<ProjectRoutingInfo> linked = List.of(createRandomProjectWithAlias("P1"), createRandomProjectWithAlias("P2"));

        Map<String, List<String>> canonical = IndexExpressionsRewriter.rewriteIndexExpressions(origin, linked, null);

        assertThat(canonical.keySet(), containsInAnyOrder("_all"));
        assertThat(canonical.get("_all"), containsInAnyOrder("P1:_all", "P2:_all", "_all"));
    }

    public void testWildcardExpressionShouldMatchAll() {
        ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        List<ProjectRoutingInfo> linked = List.of(createRandomProjectWithAlias("P1"), createRandomProjectWithAlias("P2"));
        String[] requestedResources = new String[] { "*" };

        Map<String, List<String>> canonical = IndexExpressionsRewriter.rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(canonical.keySet(), containsInAnyOrder("*"));
        assertThat(canonical.get("*"), containsInAnyOrder("P1:*", "P2:*", "*"));
    }

    public void test_ALLExpressionShouldMatchAll() {
        ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        List<ProjectRoutingInfo> linked = List.of(createRandomProjectWithAlias("P1"), createRandomProjectWithAlias("P2"));
        String all = randomBoolean() ? "_ALL" : "_all";
        String[] requestedResources = new String[] { all };

        Map<String, List<String>> canonical = IndexExpressionsRewriter.rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(canonical.keySet(), containsInAnyOrder(all));
        assertThat(canonical.get(all), containsInAnyOrder("P1:" + all, "P2:" + all, all));
    }

    public void testRewritingShouldThrowIfNotProjectMatchExpression() {
        ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        List<ProjectRoutingInfo> linked = List.of(
            createRandomProjectWithAlias("P1"),
            createRandomProjectWithAlias("P2"),
            createRandomProjectWithAlias("Q1"),
            createRandomProjectWithAlias("Q2")
        );
        String[] requestedResources = new String[] { "X*:metrics" };

        expectThrows(
            NoMatchingProjectException.class,
            () -> IndexExpressionsRewriter.rewriteIndexExpressions(origin, linked, requestedResources)
        );
    }

    private ProjectRoutingInfo createRandomProjectWithAlias(String alias) {
        ProjectId projectId = randomUniqueProjectId();
        String type = randomFrom("elasticsearch", "security", "observability");
        String org = randomAlphaOfLength(10);

        Map<String, String> tags = Map.of("_id", projectId.id(), "_type", type, "_organization", org, "_alias", alias);
        ProjectTags projectTags = new ProjectTags(tags);
        return new ProjectRoutingInfo(projectId, type, alias, org, projectTags);
    }
}
