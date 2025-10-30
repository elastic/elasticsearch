/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.crossproject;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Assumptions to clarify:
 * 1. p* means "starting with 'p'" and not the regex.
 * 2. *p means "ending in 'p'" and not the regex.
 * 3. * means "return everything".
 * 4. An unsupported projectRouting is treated as 400 Bad Requests. This is also true for projectRouting we plan to support.
 */
public class CrossProjectRoutingResolverTests extends ESTestCase {

    @ParametersFactory(shuffle = false)
    public static Iterable<Object[]> parameters() {
        return Stream.of(
            testCase(null, "my_project_alias", List.of("project_2", "oh_snap"), List.of("my_project_alias", "project_2", "oh_snap")),
            testCase("", "my_project_alias", List.of("project_2", "oh_snap"), List.of("my_project_alias", "project_2", "oh_snap")),
            testCase("_alias:my_project_alias", "my_project_alias", List.of("project_2", "oh_snap"), List.of("my_project_alias")),
            testCase("_alias:p*", "my_project_alias", List.of("project_2", "oh_snap"), List.of("project_2")),
            testCase("_alias:*p", "my_project_alias", List.of("project_2", "oh_snap"), List.of("oh_snap")),
            testCase("_alias:*", "my_project_alias", List.of("project_2", "oh_snap"), List.of("my_project_alias", "project_2", "oh_snap")),
            testCase("_alias:*s*", "my_project_alias", List.of("oh_snap"), List.of("my_project_alias", "oh_snap")),
            testCase("_alias:_origin", "my_project_alias", List.of("project_2", "oh_snap"), List.of("my_project_alias")),
            testCase("_alias:hello", "my_project_alias", List.of("project_2", "oh_snap"), List.of()),
            testCase(
                "_alias:",
                "my_project_alias",
                List.of("project_2", "oh_snap"),
                new ElasticsearchStatusException("project_routing expression [_alias:] cannot be empty", RestStatus.BAD_REQUEST)
            ),
            testCase(
                "hello",
                "my_project_alias",
                List.of("project_2", "oh_snap"),
                new ElasticsearchStatusException("project_routing [hello] must start with the prefix [_alias:]", RestStatus.BAD_REQUEST)
            ),
            testCase(
                "_region:us-east-1",
                "my_project_alias",
                List.of("project_2", "oh_snap"),
                new ElasticsearchStatusException(
                    "Unsupported tag [_region] in project_routing expression [_region:us-east-1]. Supported tags [_alias].",
                    RestStatus.BAD_REQUEST
                )
            ),
            testCase("_alias:pro*_2", "my_project_alias", List.of("project_2", "oh_snap"), List.of("project_2")),
            testCase(
                "_alias:*pro*_2",
                "my_project_alias",
                List.of("project_2", "oh_snap"),
                new ElasticsearchStatusException(
                    "Unsupported project_routing expression [*pro*_2]. "
                        + "Tech Preview only supports project routing via a single project alias or wildcard alias expression",
                    RestStatus.BAD_REQUEST
                )
            ),
            testCase(
                "_alias:_origin OR _alias:*p",
                "my_project_alias",
                List.of("project_2", "oh_snap"),
                new ElasticsearchStatusException(
                    "Unsupported project_routing expression [_origin OR _alias:*p]. "
                        + "Tech Preview only supports project routing via a single project alias or wildcard alias expression",
                    RestStatus.BAD_REQUEST
                )
            ),
            testCase(
                "_alias:_origin AND *p",
                "my_project_alias",
                List.of("project_2", "oh_snap"),
                new ElasticsearchStatusException(
                    "Unsupported project_routing expression [_origin AND *p]. "
                        + "Tech Preview only supports project routing via a single project alias or wildcard alias expression",
                    RestStatus.BAD_REQUEST
                )
            ),
            testCase(
                "_alias:_origin\tAND\t*p",
                "my_project_alias",
                List.of("project_2", "oh_snap"),
                new ElasticsearchStatusException(
                    "Unsupported project_routing expression [_origin\tAND\t*p]. "
                        + "Tech Preview only supports project routing via a single project alias or wildcard alias expression",
                    RestStatus.BAD_REQUEST
                )
            ),
            testCase("_alias:**", "my_project_alias", List.of("project_2", "oh_snap"), List.of("my_project_alias", "project_2", "oh_snap")),
            testCase(
                "_alias:_origin\tAND\t*p",
                "my_project_alias",
                List.of("project_2", "oh_snap"),
                new ElasticsearchStatusException(
                    "Unsupported project_routing expression [_origin\tAND\t*p]. "
                        + "Tech Preview only supports project routing via a single project alias or wildcard alias expression",
                    RestStatus.BAD_REQUEST
                )
            ),
            testCase(
                "_alias:_origin\tAND\t*p",
                "my_project_alias",
                List.of("project_2", "oh_snap"),
                new ElasticsearchStatusException(
                    "Unsupported project_routing expression [_origin\tAND\t*p]. "
                        + "Tech Preview only supports project routing via a single project alias or wildcard alias expression",
                    RestStatus.BAD_REQUEST
                )
            ),
            testCase("_alias:_*", "my_project_alias", List.of("project_2", "oh_snap"), List.of()),
            testCase("_alias:*in", "my_project_alias", List.of("project_2", "oh_snap"), List.of()),
            testCase("_alias:*or*", "my_project_alias", List.of("project_2", "oh_snap"), List.of()),
            testCase("_alias:_alias", "my_project_alias", List.of("project_2", "oh_snap"), List.of()),
            testCase("_alias:*oh_SNAP", "my_project_alias", List.of("project_2", "oh_snap"), List.of("oh_snap")),
            testCase("_alias:oh_SNAP*", "my_project_alias", List.of("project_2", "oh_snap"), List.of("oh_snap")),
            testCase("_alias:*oh_SNAP*", "my_project_alias", List.of("project_2", "oh_snap"), List.of("oh_snap")),
            testCase("_alias:*SNAP", "my_project_alias", List.of("project_2", "oh_snap"), List.of("oh_snap")),
            testCase("_alias:oh_SN*", "my_project_alias", List.of("project_2", "oh_snap"), List.of("oh_snap")),
            testCase("_alias:*SN*", "my_project_alias", List.of("project_2", "oh_snap"), List.of("oh_snap")),
            testCase(
                "_alias: myalias",
                "my_project_alias",
                List.of("project_2", "oh_snap"),
                new ElasticsearchStatusException(
                    "Unsupported project_routing expression [ myalias]. "
                        + "Tech Preview only supports project routing via a single project alias or wildcard alias expression",
                    RestStatus.BAD_REQUEST
                )
            ),
            testCase(
                "_alias :myalias",
                "my_project_alias",
                List.of("project_2", "oh_snap"),
                new ElasticsearchStatusException(
                    "Unsupported tag [_alias ] in project_routing expression [_alias :myalias]. Supported tags [_alias].",
                    RestStatus.BAD_REQUEST
                )
            )
        ).toList();
    }

    private static final CrossProjectRoutingResolver crossProjectRoutingResolver = new CrossProjectRoutingResolver();
    private final TestCase testCase;

    public CrossProjectRoutingResolverTests(TestCase testCase) {
        this.testCase = testCase;
    }

    public void test() {
        if (testCase.expectedResolvedProjectAliases != null && testCase.expectedResolvedProjectAliases.isEmpty()) {
            assertThat(
                crossProjectRoutingResolver.resolve(
                    testCase.projectRouting,
                    new TargetProjects(testCase.originProject, testCase.candidateProjects)
                ),
                is(TargetProjects.EMPTY)
            );
        } else if (testCase.expectedResolvedProjectAliases != null) {
            assertThat(
                crossProjectRoutingResolver.resolve(
                    testCase.projectRouting,
                    new TargetProjects(testCase.originProject, testCase.candidateProjects)
                ).allProjectAliases(),
                Matchers.containsInAnyOrder(testCase.expectedResolvedProjectAliases.toArray())
            );
        } else if (testCase.expectedException != null) {
            var actualException = assertThrows(
                ElasticsearchStatusException.class,
                () -> crossProjectRoutingResolver.resolve(
                    testCase.projectRouting,
                    new TargetProjects(testCase.originProject, testCase.candidateProjects)
                )
            );
            assertThat(actualException.getMessage(), equalTo(testCase.expectedException.getMessage()));
            assertThat(actualException.status(), equalTo(testCase.expectedException.status()));
        } else {
            fail("Either expectedResolvedProjectAliases or expectedException must be provided");
        }
    }

    private static Object[] testCase(
        String projectRouting,
        String originProjectAlias,
        List<String> projectAliases,
        List<String> expectedResolvedProjectAliases
    ) {
        return new Object[] {
            new TestCase(
                projectRouting,
                projectWithAlias(originProjectAlias),
                projectAliases.stream().map(CrossProjectRoutingResolverTests::projectWithAlias).toList(),
                expectedResolvedProjectAliases,
                null
            ) };
    }

    private static ProjectRoutingInfo projectWithAlias(String alias) {
        return new ProjectRoutingInfo(ProjectId.DEFAULT, "projectType", alias, "organizationId", new ProjectTags(Map.of()));
    }

    private static Object[] testCase(
        String projectRouting,
        String originProjectAlias,
        List<String> projectAliases,
        ElasticsearchStatusException expectedException
    ) {
        return new Object[] {
            new TestCase(
                projectRouting,
                projectWithAlias(originProjectAlias),
                projectAliases.stream().map(CrossProjectRoutingResolverTests::projectWithAlias).toList(),
                null,
                expectedException
            ) };
    }

    private record TestCase(
        String projectRouting,
        ProjectRoutingInfo originProject,
        List<ProjectRoutingInfo> candidateProjects,
        List<String> expectedResolvedProjectAliases,
        ElasticsearchStatusException expectedException
    ) {
        private TestCase {
            Objects.requireNonNull(originProject);
            Objects.requireNonNull(candidateProjects);
            assert expectedResolvedProjectAliases != null ^ expectedException != null;
        }
    }
}
