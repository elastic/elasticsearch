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
import org.elasticsearch.action.support.IndexComponentSelector;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.search.crossproject.CrossProjectIndexExpressionsRewriter.MATCH_ALL;
import static org.elasticsearch.search.crossproject.CrossProjectIndexExpressionsRewriter.getAllProjectAliases;
import static org.elasticsearch.search.crossproject.CrossProjectIndexExpressionsRewriter.rewriteIndexExpression;
import static org.elasticsearch.search.crossproject.CrossProjectIndexExpressionsRewriter.validateIndexExpressionWithoutRewrite;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class CrossProjectIndexExpressionsRewriterTests extends ESTestCase {

    public void testFlatOnlyRewrite() {
        ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        List<ProjectRoutingInfo> linked = List.of(
            createRandomProjectWithAlias("P1"),
            createRandomProjectWithAlias("P2"),
            createRandomProjectWithAlias("P3")
        );
        String[] requestedResources = new String[] { "logs*", "metrics*", "<traces-{now/d}>" };

        var actual = rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(actual.keySet(), containsInAnyOrder("logs*", "metrics*", "<traces-{now/d}>"));
        assertIndexRewriteResultsContains(actual.get("logs*"), containsInAnyOrder("logs*", "P1:logs*", "P2:logs*", "P3:logs*"));
        assertIndexRewriteResultsContains(
            actual.get("metrics*"),
            containsInAnyOrder("metrics*", "P1:metrics*", "P2:metrics*", "P3:metrics*")
        );
        assertIndexRewriteResultsContains(
            actual.get("<traces-{now/d}>"),
            containsInAnyOrder("<traces-{now/d}>", "P1:<traces-{now/d}>", "P2:<traces-{now/d}>", "P3:<traces-{now/d}>")
        );
    }

    public void testFlatAndQualifiedRewrite() {
        ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        List<ProjectRoutingInfo> linked = List.of(
            createRandomProjectWithAlias("P1"),
            createRandomProjectWithAlias("P2"),
            createRandomProjectWithAlias("P3")
        );
        String[] requestedResources = new String[] { "P1:logs*", "metrics*", "P2:<traces-{now/d}>" };

        var actual = rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(actual.keySet(), containsInAnyOrder("P1:logs*", "metrics*", "P2:<traces-{now/d}>"));
        assertIndexRewriteResultsContains(actual.get("P1:logs*"), containsInAnyOrder("P1:logs*"));
        assertIndexRewriteResultsContains(
            actual.get("metrics*"),
            containsInAnyOrder("metrics*", "P1:metrics*", "P2:metrics*", "P3:metrics*")
        );
        assertIndexRewriteResultsContains(actual.get("P2:<traces-{now/d}>"), containsInAnyOrder("P2:<traces-{now/d}>"));
    }

    public void testQualifiedOnlyRewrite() {
        ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        List<ProjectRoutingInfo> linked = List.of(
            createRandomProjectWithAlias("P1"),
            createRandomProjectWithAlias("P2"),
            createRandomProjectWithAlias("P3")
        );
        String[] requestedResources = new String[] { "P1:logs*", "P2:metrics*", "P3:<traces-{now/d}>" };

        var actual = rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(actual.keySet(), containsInAnyOrder("P1:logs*", "P2:metrics*", "P3:<traces-{now/d}>"));
        assertIndexRewriteResultsContains(actual.get("P1:logs*"), containsInAnyOrder("P1:logs*"));
        assertIndexRewriteResultsContains(actual.get("P2:metrics*"), containsInAnyOrder("P2:metrics*"));
        assertIndexRewriteResultsContains(actual.get("P3:<traces-{now/d}>"), containsInAnyOrder("P3:<traces-{now/d}>"));
    }

    public void testOriginQualifiedOnlyRewrite() {
        ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        List<ProjectRoutingInfo> linked = List.of(
            createRandomProjectWithAlias("P1"),
            createRandomProjectWithAlias("P2"),
            createRandomProjectWithAlias("P3")
        );
        String[] requestedResources = new String[] { "_origin:logs*", "_origin:metrics*", "_origin:<traces-{now/d}>" };

        var actual = rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(actual.keySet(), containsInAnyOrder("_origin:logs*", "_origin:metrics*", "_origin:<traces-{now/d}>"));
        assertIndexRewriteResultsContains(actual.get("_origin:logs*"), containsInAnyOrder("logs*"));
        assertIndexRewriteResultsContains(actual.get("_origin:metrics*"), containsInAnyOrder("metrics*"));
        assertIndexRewriteResultsContains(actual.get("_origin:<traces-{now/d}>"), containsInAnyOrder("<traces-{now/d}>"));
    }

    public void testOriginQualifiedOnlyRewriteWithNoLikedProjects() {
        ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        List<ProjectRoutingInfo> linked = List.of();
        String[] requestedResources = new String[] { "_origin:logs*", "_origin:metrics*" };

        var actual = rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(actual.keySet(), containsInAnyOrder("_origin:logs*", "_origin:metrics*"));
        assertIndexRewriteResultsContains(actual.get("_origin:logs*"), containsInAnyOrder("logs*"));
        assertIndexRewriteResultsContains(actual.get("_origin:metrics*"), containsInAnyOrder("metrics*"));
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

        var actual = rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(actual.keySet(), containsInAnyOrder(logResource, metricResource));
        assertIndexRewriteResultsContains(actual.get(logResource), containsInAnyOrder(logIndexAlias));
        assertIndexRewriteResultsContains(actual.get(metricResource), containsInAnyOrder(metricsIndexAlias));
    }

    public void testQualifiedLinkedAndOriginRewrite() {
        ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        List<ProjectRoutingInfo> linked = List.of(
            createRandomProjectWithAlias("P1"),
            createRandomProjectWithAlias("P2"),
            createRandomProjectWithAlias("P3")
        );
        String[] requestedResources = new String[] { "P1:logs*", "_origin:metrics*" };

        var actual = rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(actual.keySet(), containsInAnyOrder("P1:logs*", "_origin:metrics*"));
        assertIndexRewriteResultsContains(actual.get("P1:logs*"), containsInAnyOrder("P1:logs*"));
        assertIndexRewriteResultsContains(actual.get("_origin:metrics*"), containsInAnyOrder("metrics*"));
    }

    public void testQualifiedStartsWithProjectWildcardRewrite() {
        ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        List<ProjectRoutingInfo> linked = List.of(
            createRandomProjectWithAlias("P1"),
            createRandomProjectWithAlias("P2"),
            createRandomProjectWithAlias("Q1"),
            createRandomProjectWithAlias("Q2")
        );
        String[] requestedResources = new String[] { "Q*:metrics*", "P*:<traces-{now/d}>" };

        var actual = rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(actual.keySet(), containsInAnyOrder("Q*:metrics*", "P*:<traces-{now/d}>"));
        assertIndexRewriteResultsContains(actual.get("Q*:metrics*"), containsInAnyOrder("Q1:metrics*", "Q2:metrics*"));
        assertIndexRewriteResultsContains(
            actual.get("P*:<traces-{now/d}>"),
            containsInAnyOrder("<traces-{now/d}>", "P1:<traces-{now/d}>", "P2:<traces-{now/d}>")
        );
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

        var actual = rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(actual.keySet(), containsInAnyOrder("*1:metrics*"));
        assertIndexRewriteResultsContains(actual.get("*1:metrics*"), containsInAnyOrder("P1:metrics*", "Q1:metrics*"));
    }

    public void testOriginProjectMatchingTwice() {
        ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        List<ProjectRoutingInfo> linked = List.of(createRandomProjectWithAlias("P1"), createRandomProjectWithAlias("P2"));
        String[] requestedResources = new String[] { "P0:metrics*", "_origin:metrics*" };

        var actual = rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(actual.keySet(), containsInAnyOrder("P0:metrics*", "_origin:metrics*"));
        assertIndexRewriteResultsContains(actual.get("P0:metrics*"), containsInAnyOrder("metrics*"));
        assertIndexRewriteResultsContains(actual.get("_origin:metrics*"), containsInAnyOrder("metrics*"));
    }

    public void testUnderscoreWildcardShouldNotMatchOrigin() {
        ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        List<ProjectRoutingInfo> linked = List.of(createRandomProjectWithAlias("_P1"), createRandomProjectWithAlias("_P2"));
        String[] requestedResources = new String[] { "_*:metrics*" };

        var actual = rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(actual.keySet(), containsInAnyOrder("_*:metrics*"));
        assertIndexRewriteResultsContains(actual.get("_*:metrics*"), containsInAnyOrder("_P1:metrics*", "_P2:metrics*"));
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

        var actual = rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(actual.keySet(), containsInAnyOrder(indexPattern));
        assertIndexRewriteResultsContains(actual.get(indexPattern), containsInAnyOrder("Q1:metrics*", "Q2:metrics*"));
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

        expectThrows(ResourceNotFoundException.class, () -> rewriteIndexExpressions(origin, linked, requestedResources));
    }

    public void testRewritingWithIndexExclusions() {
        final ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        final List<ProjectRoutingInfo> linked = List.of(
            createRandomProjectWithAlias("P1"),
            createRandomProjectWithAlias("P2"),
            createRandomProjectWithAlias("Q1"),
            createRandomProjectWithAlias("Q2")
        );

        {
            // Unqualified exclusion
            final var excludeExpression = "-" + randomFrom("metrics*", "metrics");
            final var requestedResources = new String[] { "*", excludeExpression };
            final var actual = rewriteIndexExpressions(origin, linked, requestedResources);

            assertThat(actual.keySet(), containsInAnyOrder("*", excludeExpression));
            assertIndexRewriteResultsContains(actual.get("*"), containsInAnyOrder("*", "P1:*", "P2:*", "Q1:*", "Q2:*"));
            assertIndexRewriteResultsContains(
                actual.get(excludeExpression),
                containsInAnyOrder(
                    excludeExpression,
                    "P1:" + excludeExpression,
                    "P2:" + excludeExpression,
                    "Q1:" + excludeExpression,
                    "Q2:" + excludeExpression
                )
            );
        }

        {
            // Exclusion on origin project or index
            final String excludedIndex = randomFrom("metrics*", "metrics");
            final var excludeExpression = randomBoolean() ? "-_origin:" + excludedIndex : "_origin:-" + excludedIndex;
            final var requestedResources = new String[] { "*", excludeExpression };
            final var actual = rewriteIndexExpressions(origin, linked, requestedResources);

            assertThat(actual.keySet(), containsInAnyOrder("*", excludeExpression));
            assertIndexRewriteResultsContains(actual.get("*"), containsInAnyOrder("*", "P1:*", "P2:*", "Q1:*", "Q2:*"));
            assertIndexRewriteResultsContains(actual.get(excludeExpression), containsInAnyOrder("-" + excludedIndex));
        }

        {
            // Exclusion on remote project or index
            final String excludedProject = "P1";
            final String excludedIndex = randomFrom("*", "metrics*", "metrics");
            final var excludeExpression = randomBoolean()
                ? "-" + excludedProject + ":" + excludedIndex
                : excludedProject + ":-" + excludedIndex;
            final var requestedResources = new String[] { "*", excludeExpression };
            final var actual = rewriteIndexExpressions(origin, linked, requestedResources);

            assertThat(actual.keySet(), containsInAnyOrder("*", excludeExpression));
            assertIndexRewriteResultsContains(actual.get("*"), containsInAnyOrder("*", "P1:*", "P2:*", "Q1:*", "Q2:*"));
            assertIndexRewriteResultsContains(actual.get(excludeExpression), containsInAnyOrder(excludeExpression));
        }

        {
            // Exclusion on origin project or index throws 404 if origin is filtered out by project routing
            final String excludedIndex = randomFrom("*", "metrics*", "metrics");
            final var excludeExpression = randomBoolean() ? "-_origin:" + excludedIndex : "_origin:-" + excludedIndex;
            final var requestedResources = new String[] { "*", excludeExpression };
            expectThrows(NoMatchingProjectException.class, () -> rewriteIndexExpressions(null, linked, requestedResources));
        }

        {
            // Exclusion on unmatched remote project or index throws 404
            final String excludedProject = "X" + (randomBoolean() ? "*" : ""); // does not exist
            final String excludedIndex = randomFrom("*", "metrics*", "metrics");
            final var excludeExpression = randomBoolean()
                ? "-" + excludedProject + ":" + excludedIndex
                : excludedProject + ":-" + excludedIndex;
            final var requestedResources = new String[] { "*", excludeExpression };
            expectThrows(NoMatchingProjectException.class, () -> rewriteIndexExpressions(origin, linked, requestedResources));
        }

        {
            // Empty exclusion on remote projects throws 404
            final var requestedResources = new String[] { "*", "-:metrics*" };
            expectThrows(NoMatchingProjectException.class, () -> rewriteIndexExpressions(origin, linked, requestedResources));
        }

        {
            // Cannot apply exclusion for both the project and the index
            final var messageMatcher = containsString("Cannot apply exclusion for both the project and the index expression");
            expectThrows(
                IllegalArgumentException.class,
                messageMatcher,
                () -> rewriteIndexExpressions(origin, linked, "-_origin:-metrics*")
            );
            expectThrows(IllegalArgumentException.class, messageMatcher, () -> rewriteIndexExpressions(origin, linked, "-P0:-metrics*"));
            expectThrows(IllegalArgumentException.class, messageMatcher, () -> rewriteIndexExpressions(origin, linked, "-P1:-metrics*"));
        }
    }

    public void testRewritingShouldWorkWithIndexSelectors() {
        ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        List<ProjectRoutingInfo> linked = List.of(
            createRandomProjectWithAlias("P1"),
            createRandomProjectWithAlias("P2"),
            createRandomProjectWithAlias("Q1"),
            createRandomProjectWithAlias("Q2")
        );
        final var selector = randomFrom(IndexComponentSelector.values()).getKey();
        String[] requestedResources = new String[] { "index::" + selector };

        var actual = rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(actual.keySet(), containsInAnyOrder("index::" + selector));
        assertIndexRewriteResultsContains(
            actual.get("index::" + selector),
            containsInAnyOrder(
                "index::" + selector,
                "P1:index::" + selector,
                "P2:index::" + selector,
                "Q1:index::" + selector,
                "Q2:index::" + selector
            )
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

        var actual = rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(actual.keySet(), containsInAnyOrder("*:metrics*"));
        assertIndexRewriteResultsContains(
            actual.get("*:metrics*"),
            containsInAnyOrder("P1:metrics*", "P2:metrics*", "Q1:metrics*", "Q2:metrics*", "metrics*")
        );
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

        var actual = rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(actual.keySet(), containsInAnyOrder("alias*:metrics*"));
        assertIndexRewriteResultsContains(actual.get("alias*:metrics*"), containsInAnyOrder("metrics*"));
    }

    public void testEmptyExpressionShouldMatchAll() {
        ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        List<ProjectRoutingInfo> linked = List.of(createRandomProjectWithAlias("P1"), createRandomProjectWithAlias("P2"));
        String[] requestedResources = new String[] {};

        var actual = rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(actual.keySet(), containsInAnyOrder("_all"));
        assertIndexRewriteResultsContains(actual.get("_all"), containsInAnyOrder("P1:_all", "P2:_all", "_all"));
    }

    public void testNullExpressionShouldMatchAll() {
        ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        List<ProjectRoutingInfo> linked = List.of(createRandomProjectWithAlias("P1"), createRandomProjectWithAlias("P2"));

        var actual = rewriteIndexExpressions(origin, linked);

        assertThat(actual.keySet(), containsInAnyOrder("_all"));
        assertIndexRewriteResultsContains(actual.get("_all"), containsInAnyOrder("P1:_all", "P2:_all", "_all"));
    }

    public void testWildcardExpressionShouldMatchAll() {
        ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        List<ProjectRoutingInfo> linked = List.of(createRandomProjectWithAlias("P1"), createRandomProjectWithAlias("P2"));
        String[] requestedResources = new String[] { "*" };

        var actual = rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(actual.keySet(), containsInAnyOrder("*"));
        assertIndexRewriteResultsContains(actual.get("*"), containsInAnyOrder("P1:*", "P2:*", "*"));
    }

    public void test_ALLExpressionShouldMatchAll() {
        ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
        List<ProjectRoutingInfo> linked = List.of(createRandomProjectWithAlias("P1"), createRandomProjectWithAlias("P2"));
        String all = randomBoolean() ? "_ALL" : "_all";
        String[] requestedResources = new String[] { all };

        var actual = rewriteIndexExpressions(origin, linked, requestedResources);

        assertThat(actual.keySet(), containsInAnyOrder(all));
        assertIndexRewriteResultsContains(actual.get(all), containsInAnyOrder("P1:" + all, "P2:" + all, all));
    }

    public void testRewritingShouldThrowIfNotProjectMatchExpression() {
        {
            final var projectRouting = "_alias:" + randomAlphaOfLengthBetween(1, 10);

            final var e = expectThrows(
                NoMatchingProjectException.class,
                () -> rewriteIndexExpression(randomIdentifier(), null, Set.of(), projectRouting)
            );
            assertThat(e.getMessage(), equalTo("no matching project after applying project routing [" + projectRouting + "]"));
        }

        {
            ProjectRoutingInfo origin = createRandomProjectWithAlias("P0");
            List<ProjectRoutingInfo> linked = List.of(
                createRandomProjectWithAlias("P1"),
                createRandomProjectWithAlias("P2"),
                createRandomProjectWithAlias("Q1"),
                createRandomProjectWithAlias("Q2")
            );
            String indexExpression = "X*:metrics";
            final var projectRouting = randomBoolean() ? "_alias:" + randomAlphaOfLengthBetween(1, 10) : null;

            final var e = expectThrows(
                NoMatchingProjectException.class,
                () -> rewriteIndexExpression(
                    indexExpression,
                    origin.projectAlias(),
                    linked.stream().map(ProjectRoutingInfo::projectAlias).collect(Collectors.toUnmodifiableSet()),
                    projectRouting
                )
            );

            if (projectRouting != null) {
                assertThat(e.getMessage(), equalTo("No such project: [X*] with project routing [" + projectRouting + "]"));
            } else {
                assertThat(e.getMessage(), equalTo("No such project: [X*]"));
            }
        }
    }

    public void testValidateIndexExpressionWithoutRewrite() {
        var origin = createRandomProjectWithAlias("P0");
        var linked = List.of(createRandomProjectWithAlias("P1"), createRandomProjectWithAlias("P2"));
        var allProjectAliases = getAllProjectAliases(origin, linked);

        // unqualified expression should pass validation without checking projects
        validateIndexExpressionWithoutRewrite("logs*", origin.projectAlias(), randomBoolean() ? allProjectAliases : Set.of(), null);

        // qualified non-exclusion referencing existing origin or linked projects should pass
        validateIndexExpressionWithoutRewrite(randomFrom("P1:logs*", "P0:logs*"), origin.projectAlias(), allProjectAliases, null);

        // qualified expression referencing _origin when origin exists should pass
        validateIndexExpressionWithoutRewrite("_origin:logs*", origin.projectAlias(), allProjectAliases, null);

        // wildcard project alias matching existing projects should pass
        validateIndexExpressionWithoutRewrite("P*:logs*", origin.projectAlias(), allProjectAliases, null);

        // exclusion referencing existing projects should not throw validation exception
        validateIndexExpressionWithoutRewrite(randomFrom("-P1:logs*", "-P0:logs*"), origin.projectAlias(), allProjectAliases, null);

        // qualified non-exclusion referencing missing project should fail validation
        expectThrows(
            NoMatchingProjectException.class,
            containsString("No such project: [missing]"),
            () -> validateIndexExpressionWithoutRewrite("missing:logs*", origin.projectAlias(), allProjectAliases, null)
        );

        // exclusion referencing a non-existing project should fail validation
        expectThrows(
            NoMatchingProjectException.class,
            containsString("No such project: [missing]"),
            () -> validateIndexExpressionWithoutRewrite("-missing:_all", origin.projectAlias(), allProjectAliases, null)
        );

        // wildcard project alias not matching any project should fail validation
        expectThrows(
            NoMatchingProjectException.class,
            containsString("No such project: [Q*]"),
            () -> validateIndexExpressionWithoutRewrite("Q*:logs*", origin.projectAlias(), allProjectAliases, null)
        );

        // _origin qualified expression when origin is null should fail validation
        expectThrows(
            NoMatchingProjectException.class,
            containsString("No such project: [_origin]"),
            () -> validateIndexExpressionWithoutRewrite("_origin:logs*", null, allProjectAliases, null)
        );

        // no projects available should fail validation
        var projectRouting = "_alias:" + randomAlphaOfLengthBetween(1, 10);
        expectThrows(
            NoMatchingProjectException.class,
            containsString("no matching project after applying project routing [" + projectRouting + "]"),
            () -> validateIndexExpressionWithoutRewrite("P1:logs*", null, Set.of(), projectRouting)
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

    private static void assertIndexRewriteResultsContains(
        CrossProjectIndexExpressionsRewriter.IndexRewriteResult actual,
        Matcher<Iterable<?>> iterableMatcher
    ) {
        assertThat(resultAsList(actual), iterableMatcher);
    }

    private static List<String> resultAsList(CrossProjectIndexExpressionsRewriter.IndexRewriteResult result) {
        if (result.localExpression() == null) {
            return List.copyOf(result.remoteExpressions());
        }
        List<String> all = new ArrayList<>();
        all.add(result.localExpression());
        all.addAll(result.remoteExpressions());
        return List.copyOf(all);
    }

    /**
     * Rewrites index expressions for cross-project search requests.
     * Handles qualified and unqualified expressions and match-all cases will also hand exclusions in the future.
     *
     * @param originProject the _origin project with its alias
     * @param linkedProjects the list of linked and available projects to consider for a request
     * @param originalIndices the array of index expressions to be rewritten to canonical CCS
     * @return a map from original index expressions to lists of canonical index expressions
     * @throws IllegalArgumentException if exclusions, date math or selectors are present in the index expressions
     * @throws NoMatchingProjectException if a qualified resource cannot be resolved because a project is missing
     */
    private static Map<String, CrossProjectIndexExpressionsRewriter.IndexRewriteResult> rewriteIndexExpressions(
        ProjectRoutingInfo originProject,
        List<ProjectRoutingInfo> linkedProjects,
        final String... originalIndices
    ) {
        final String[] indices;
        if (originalIndices == null || originalIndices.length == 0) { // handling of match all cases besides _all and `*`
            indices = MATCH_ALL;
        } else {
            indices = originalIndices;
        }
        assert false == IndexNameExpressionResolver.isNoneExpression(indices)
            : "expression list is *,-* which effectively means a request that requests no indices";

        final Set<String> allProjectAliases = getAllProjectAliases(originProject, linkedProjects);
        final String originProjectAlias = originProject != null ? originProject.projectAlias() : null;
        final Map<String, CrossProjectIndexExpressionsRewriter.IndexRewriteResult> canonicalExpressionsMap = new LinkedHashMap<>(
            indices.length
        );
        for (String indexExpression : indices) {
            if (canonicalExpressionsMap.containsKey(indexExpression)) {
                continue;
            }
            canonicalExpressionsMap.put(
                indexExpression,
                rewriteIndexExpression(indexExpression, originProjectAlias, allProjectAliases, null)
            );
        }
        return canonicalExpressionsMap;
    }
}
