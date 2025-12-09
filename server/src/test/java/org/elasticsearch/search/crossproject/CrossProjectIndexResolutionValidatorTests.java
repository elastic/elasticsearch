/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.crossproject;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ResolvedIndexExpression;
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class CrossProjectIndexResolutionValidatorTests extends ESTestCase {

    private boolean useProjectRouting;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        useProjectRouting = randomBoolean();
    }

    public void testLenientIndicesOptions() {
        // with lenient IndicesOptions we early terminate without error
        assertNull(CrossProjectIndexResolutionValidator.validate(getLenientIndicesOptions(), randomFrom("_alias:*", null), null, null));
    }

    public void testFlatExpressionWithStrictIgnoreUnavailableMatchingInOriginProject() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "logs",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of("logs"),
                        ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS,
                        null
                    ),
                    Set.of("P1:logs")
                )
            )
        );

        // we matched resource locally thus no error
        assertNull(
            CrossProjectIndexResolutionValidator.validate(
                getStrictIgnoreUnavailable(),
                useProjectRouting ? "_alias:*" : null,  // a redundant project routing has no impact
                local,
                null
            )
        );
    }

    public void testFlatExpressionWithStrictIgnoreUnavailableMatchingInLinkedProject() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "logs",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of(),
                        ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE,
                        null
                    ),
                    Set.of("P1:logs")
                )
            )
        );

        var remote = Map.of(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of("logs"),
                            ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS,
                            null
                        ),
                        Set.of()
                    )
                )
            )
        );

        // we matched the flat resource in a linked project thus no error
        assertNull(
            CrossProjectIndexResolutionValidator.validate(
                getStrictIgnoreUnavailable(),
                useProjectRouting ? "_alias:*" : null,  // a redundant project routing has no impact
                local,
                remote
            )
        );
    }

    public void testMissingFlatExpressionWithStrictIgnoreUnavailable() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "logs",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of(),
                        ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE,
                        null
                    ),
                    Set.of("P1:logs")
                )
            )
        );

        var remote = Map.of(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE,
                            null
                        ),
                        Set.of()
                    )
                )
            )
        );
        var e = CrossProjectIndexResolutionValidator.validate(
            getStrictIgnoreUnavailable(),
            useProjectRouting ? "_alias:*" : null,  // a redundant project routing has no impact
            local,
            remote
        );
        assertNotNull(e);
        assertThat(e, instanceOf(IndexNotFoundException.class));
        assertThat(e.getMessage(), containsString("no such index [logs]"));
    }

    public void testMissingResponseFromLinkedProjectsWithStrictIgnoreUnavailable() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "logs",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of(),
                        ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE,
                        null
                    ),
                    Set.of("P1:logs")
                )
            )
        );

        // logs does not exist in the remote responses and indices options are strict. We expect an error.
        var e = CrossProjectIndexResolutionValidator.validate(
            getStrictIgnoreUnavailable(),
            useProjectRouting ? "_alias:*" : null,  // a redundant project routing has no impact
            local,
            Map.of()
        );
        assertNotNull(e);
        assertThat(e, instanceOf(IndexNotFoundException.class));
        assertThat(e.getMessage(), containsString("no such index [logs]"));
    }

    public void testMissingResponseFromLinkedProjectsWithLenientIgnoreUnavailable() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "logs",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of(),
                        ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE,
                        null
                    ),
                    Set.of("P1:logs")
                )
            )
        );

        // logs does not exist in the remote responses and ignore_unavailable is set to true. We do not expect an error.
        var e = CrossProjectIndexResolutionValidator.validate(
            getLenientIndicesOptions(),
            useProjectRouting ? "_alias:*" : null,  // a redundant project routing has no impact
            local,
            Map.of()
        );
        assertNull(e);
    }

    public void testMissingResponseFromLinkedProjectsWithStrictAllowNoIndices() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "logs*",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of(),
                        ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS,
                        null
                    ),
                    Set.of("P1:logs*")
                )
            )
        );

        // Mimic no response from P1 project.
        var remote = Map.of(
            "P2",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "not-logs*",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of("not-logs"),
                            ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS,
                            null
                        ),
                        Set.of()
                    )
                )
            )
        );

        // Index expression is a wildcard-ed expression but the indices options are strict. We expect an error.
        var e = CrossProjectIndexResolutionValidator.validate(
            getStrictAllowNoIndices(),
            useProjectRouting ? "_alias:*" : null,  // a redundant project routing has no impact
            local,
            remote
        );
        assertNotNull(e);
        assertThat(e, instanceOf(IndexNotFoundException.class));
        assertThat(e.getMessage(), containsString("no such index [logs*]"));
    }

    public void testMissingResponseFromLinkedProjectsWithLenientAllowNoIndices() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "logs*",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of(),
                        ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS,
                        null
                    ),
                    Set.of("P1:logs*")
                )
            )
        );

        // Mimic no response from P1 project.
        var remote = Map.of(
            "P2",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "not-logs*",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of("not-logs"),
                            ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS,
                            null
                        ),
                        Set.of()
                    )
                )
            )
        );

        // Index expression is a wildcard-ed expression but the indices options are lenient. We do not expect an error.
        var e = CrossProjectIndexResolutionValidator.validate(
            getLenientIndicesOptions(),
            useProjectRouting ? "_alias:*" : null,  // a redundant project routing has no impact
            local,
            remote
        );
        assertNull(e);
    }

    public void testMissingResponseFromLinkedProjectsForQualifiedExpressionWithStrictIgnoreUnavailable() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(new ResolvedIndexExpression("P1:logs", ResolvedIndexExpression.LocalExpressions.NONE, Set.of("P1:logs")))
        );

        // Mimic no response from P1 project.
        var remote = Map.of(
            "P2",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "not-logs*",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of("not-logs"),
                            ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS,
                            null
                        ),
                        Set.of()
                    )
                )
            )
        );

        // logs does not exist in the remote responses and indices options are strict. We expect an error.
        var e = CrossProjectIndexResolutionValidator.validate(
            getStrictIgnoreUnavailable(),
            useProjectRouting ? "_alias:*" : null,  // a redundant project routing has no impact
            local,
            remote
        );
        assertNotNull(e);
        assertThat(e, instanceOf(IndexNotFoundException.class));
        assertThat(e.getMessage(), containsString("no such index [P1:logs]"));
    }

    public void testMissingResponseFromLinkedProjectsForQualifiedExpressionWithLenientIgnoreUnavailable() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(new ResolvedIndexExpression("P1:logs", ResolvedIndexExpression.LocalExpressions.NONE, Set.of("P1:logs")))
        );

        // Mimic no response from P1 project.
        var remote = Map.of(
            "P2",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "not-logs*",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of("not-logs"),
                            ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS,
                            null
                        ),
                        Set.of()
                    )
                )
            )
        );

        // logs does not exist in the remote responses and indices options are lenient. We do not expect an error.
        var e = CrossProjectIndexResolutionValidator.validate(
            getLenientIndicesOptions(),
            useProjectRouting ? "_alias:P1" : null,  // a redundant project routing has no impact
            local,
            remote
        );
        assertNull(e);
    }

    public void testUnauthorizedFlatExpressionWithStrictIgnoreUnavailable() {
        final var exception = new ElasticsearchSecurityException("authorization errors while resolving [-*]");
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "logs",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of(),
                        ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED,
                        exception
                    ),
                    Set.of("P1:logs")
                )
            )
        );

        var remote = Map.of(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED,
                            new ElasticsearchSecurityException("authorization errors while resolving [logs]")
                        ),
                        Set.of()
                    )
                )
            )
        );

        var e = CrossProjectIndexResolutionValidator.validate(
            getStrictIgnoreUnavailable(),
            useProjectRouting ? "_alias:*" : null,  // a redundant project routing has no impact
            local,
            remote
        );
        assertNotNull(e);
        assertThat(e.getMessage(), equalTo("authorization errors while resolving [logs]"));
    }

    public void testUnauthorizedFlatExpressionWithStrictIgnoreUnavailableAndProjectRouting() {
        final var exception = new ElasticsearchSecurityException("authorization errors while resolving [-*]");
        final String projectRouting = "_alias:P1";
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "logs",
                    ResolvedIndexExpression.LocalExpressions.NONE, // no local resolution since it is excluded by project routing
                    Set.of("P1:logs")
                )
            )
        );

        var remote = Map.of(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED,
                            exception
                        ),
                        Set.of()
                    )
                )
            )
        );

        var e = CrossProjectIndexResolutionValidator.validate(getStrictIgnoreUnavailable(), projectRouting, local, remote);
        assertNotNull(e);
        assertThat(e.getMessage(), equalTo("authorization errors while resolving [logs]"));
    }

    public void testNotFoundFlatExpressionWithStrictIgnoreUnavailableAndProjectRouting() {
        final String projectRouting = "_alias:P1";
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "logs",
                    ResolvedIndexExpression.LocalExpressions.NONE, // no local resolution since it is excluded by project routing
                    Set.of("P1:logs")
                )
            )
        );

        var remote = Map.of(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE,
                            null
                        ),
                        Set.of()
                    )
                )
            )
        );

        var e = CrossProjectIndexResolutionValidator.validate(getStrictIgnoreUnavailable(), projectRouting, local, remote);
        assertNotNull(e);
        assertThat(e, instanceOf(IndexNotFoundException.class));
        assertThat(e.getMessage(), containsString("no such index [P1:logs]"));
    }

    public void testQualifiedExpressionWithStrictIgnoreUnavailableMatchingInOriginProject() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "_origin:logs",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of("logs"),
                        ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS,
                        null
                    ),
                    Set.of()
                )
            )
        );

        // we matched locally thus no error
        assertNull(
            CrossProjectIndexResolutionValidator.validate(
                getStrictIgnoreUnavailable(),
                useProjectRouting ? "_alias:_origin" : null, // a redundant project routing has no impact
                local,
                null
            )
        );
    }

    public void testQualifiedOriginExpressionWithStrictIgnoreUnavailableNotMatching() {
        final String original = "_origin:logs";
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    original,
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of(),
                        ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE,
                        null
                    ),
                    Set.of()
                )
            )
        );

        var e = CrossProjectIndexResolutionValidator.validate(
            getStrictIgnoreUnavailable(),
            useProjectRouting ? "_alias:_origin" : null, // a redundant project routing has no impact
            local,
            null
        );
        assertNotNull(e);
        assertThat(e, instanceOf(IndexNotFoundException.class));
        assertThat(e.getMessage(), containsString("no such index [" + original + "]"));
    }

    public void testQualifiedExpressionWithStrictIgnoreUnavailableMatchingInLinkedProject() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(new ResolvedIndexExpression("P1:logs", ResolvedIndexExpression.LocalExpressions.NONE, Set.of("P1:logs")))
        );

        var remote = Map.of(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of("logs"),
                            ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS,
                            null
                        ),
                        Set.of()
                    )
                )
            )
        );

        // we matched the flat resource in a linked project thus no error
        assertNull(
            CrossProjectIndexResolutionValidator.validate(
                getStrictIgnoreUnavailable(),
                useProjectRouting ? "_alias:P1" : null, // a redundant project routing has no impact
                local,
                remote
            )
        );
    }

    public void testMissingQualifiedExpressionWithStrictIgnoreUnavailable() {
        final String original = "P1:logs";
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    original,
                    new ResolvedIndexExpression.LocalExpressions(Set.of(), ResolvedIndexExpression.LocalIndexResolutionResult.NONE, null),
                    Set.of("P1:logs")
                )
            )
        );

        var remote = Map.of(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE,
                            null
                        ),
                        Set.of()
                    )
                )
            )
        );

        var e = CrossProjectIndexResolutionValidator.validate(
            getStrictIgnoreUnavailable(),
            useProjectRouting ? "_alias:P1" : null, // a redundant project routing has no impact
            local,
            remote
        );
        assertNotNull(e);
        assertThat(e, instanceOf(IndexNotFoundException.class));
        assertThat(e.getMessage(), containsString("no such index [P1:logs]"));
    }

    public void testUnauthorizedQualifiedExpressionWithStrictIgnoreUnavailable() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(new ResolvedIndexExpression("P1:logs", ResolvedIndexExpression.LocalExpressions.NONE, Set.of("P1:logs")))
        );

        final var exception = new ElasticsearchSecurityException("action is unauthorized for indices [-*]");
        var remote = Map.of(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED,
                            exception
                        ),
                        Set.of()
                    )
                )
            )
        );

        var e = CrossProjectIndexResolutionValidator.validate(
            getStrictIgnoreUnavailable(),
            useProjectRouting ? "_alias:P1" : null, // a redundant project routing has no impact
            local,
            remote
        );
        assertNotNull(e);
        assertThat(e.getMessage(), equalTo("action is unauthorized for indices [P1:logs]"));
    }

    public void testFlatExpressionWithStrictAllowNoIndicesMatchingInOriginProject() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "logs*",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of("logs-es"),
                        ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS,
                        null
                    ),
                    Set.of("P1:logs")
                )
            )
        );

        // we matched resource locally thus no error
        assertNull(CrossProjectIndexResolutionValidator.validate(getStrictAllowNoIndices(), null, local, null));
    }

    public void testStrictAllowNoIndicesFoundEmptyResultsOnOriginAndLinked() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "shared-index-missing*",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of(),
                        ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS,
                        null
                    ),
                    Set.of("P1:shared-index-missing*")
                )
            )
        );

        var remote = Map.of(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "shared-index-missing*",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS,
                            null
                        ),
                        Set.of()
                    )
                )
            )
        );

        ElasticsearchException ex = CrossProjectIndexResolutionValidator.validate(getIndicesOptions(false, false), null, local, remote);
        assertNotNull(ex);
        assertThat(ex, instanceOf(IndexNotFoundException.class));
    }

    public void testFlatExpressionWithStrictAllowNoIndicesMatchingInLinkedProject() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "logs*",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of(),
                        ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS,
                        null
                    ),
                    Set.of("P1:logs*")
                )
            )
        );

        var remote = Map.of(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs*",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of("logs-es"),
                            ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS,
                            null
                        ),
                        Set.of()
                    )
                )
            )
        );

        // we matched the flat resource in a linked project thus no error
        assertNull(CrossProjectIndexResolutionValidator.validate(getStrictAllowNoIndices(), null, local, remote));
    }

    public void testMissingFlatExpressionWithStrictAllowNoIndices() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "logs*",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of(),
                        ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS,
                        null
                    ),
                    Set.of("P1:logs*")
                )
            )
        );

        var remote = Map.of(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs*",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS,
                            null
                        ),
                        Set.of()
                    )
                )
            )
        );

        var e = CrossProjectIndexResolutionValidator.validate(getStrictAllowNoIndices(), null, local, remote);
        assertNotNull(e);
        assertThat(e, instanceOf(IndexNotFoundException.class));
        assertThat(e.getMessage(), containsString("no such index [logs*]"));
    }

    public void testUnauthorizedFlatExpressionWithStrictAllowNoIndices() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "logs*",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of(),
                        ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS,
                        null
                    ),
                    Set.of("P1:logs*")
                )
            )
        );

        var remote = Map.of(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs*",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS,
                            null
                        ),
                        Set.of()
                    )
                )
            )
        );

        var e = CrossProjectIndexResolutionValidator.validate(getStrictAllowNoIndices(), null, local, remote);
        assertNotNull(e);
        assertThat(e, instanceOf(IndexNotFoundException.class));
        assertThat(e.getMessage(), containsString("no such index [logs*]"));
    }

    public void testQualifiedExpressionWithStrictAllowNoIndicesMatchingInOriginProject() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "_origin:logs*",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of("logs-es"),
                        ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS,
                        null
                    ),
                    Set.of()
                )
            )
        );

        // we matched locally thus no error
        assertNull(
            CrossProjectIndexResolutionValidator.validate(
                getStrictAllowNoIndices(),
                useProjectRouting ? "_alias:_origin" : null, // a redundant project routing has no impact
                local,
                null
            )
        );
    }

    public void testQualifiedOriginExpressionWithStrictAllowNoIndicesNotMatching() {
        final String original = "_origin:logs*";
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    original,
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of(),
                        ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS,
                        null
                    ),
                    Set.of()
                )
            )
        );
        var e = CrossProjectIndexResolutionValidator.validate(
            getStrictAllowNoIndices(),
            useProjectRouting ? "_alias:_origin" : null, // a redundant project routing has no impact
            local,
            null
        );
        assertNotNull(e);
        assertThat(e, instanceOf(IndexNotFoundException.class));
        assertThat(e.getMessage(), containsString("no such index [" + original + "]"));
    }

    public void testQualifiedOriginExpressionWithWildcardAndStrictAllowNoIndicesMatching() {
        for (var indexExpression : List.of("_all", "*", "local-*")) {
            ResolvedIndexExpressions local = new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "_origin:" + indexExpression,
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of("local-index-1", "local-index-2"),
                            ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS,
                            null
                        ),
                        Set.of()
                    )
                )
            );
            assertNull(
                CrossProjectIndexResolutionValidator.validate(
                    getIndicesOptions(randomBoolean(), randomBoolean()),
                    useProjectRouting ? "_alias:_origin" : null, // a redundant project routing has no impact
                    local,
                    Map.of()
                )
            );
        }
    }

    public void testQualifiedExpressionWithStrictAllowNoIndicesMatchingInLinkedProject() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(new ResolvedIndexExpression("P1:logs*", ResolvedIndexExpression.LocalExpressions.NONE, Set.of("P1:logs*")))
        );

        var remote = Map.of(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs*",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of("logs-es"),
                            ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS,
                            null
                        ),
                        Set.of()
                    )
                )
            )
        );

        // we matched the flat resource in a linked project thus no error
        assertNull(
            CrossProjectIndexResolutionValidator.validate(
                getStrictAllowNoIndices(),
                useProjectRouting ? "_alias:P1" : null,  // a redundant project routing has no impact
                local,
                remote
            )
        );
    }

    public void testMissingQualifiedExpressionWithStrictAllowNoIndices() {
        final String original = "P1:logs*";
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    original,
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of(),
                        ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS,
                        null
                    ),
                    Set.of("P1:logs*")
                )
            )
        );

        var remote = Map.of(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs*",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS,
                            null
                        ),
                        Set.of()
                    )
                )
            )
        );

        var e = CrossProjectIndexResolutionValidator.validate(
            getStrictAllowNoIndices(),
            useProjectRouting ? "_alias:P1" : null, // a redundant project routing has no impact
            local,
            remote
        );
        assertNotNull(e);
        assertThat(e, instanceOf(IndexNotFoundException.class));
        assertThat(e.getMessage(), containsString("no such index [" + original + "]"));
    }

    public void testUnauthorizedQualifiedExpressionWithStrictAllowNoIndices() {
        final String original = "P1:logs*";
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    original,
                    new ResolvedIndexExpression.LocalExpressions(Set.of(), ResolvedIndexExpression.LocalIndexResolutionResult.NONE, null),
                    Set.of("P1:logs*")
                )
            )
        );

        var remote = Map.of(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs*",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS,
                            null
                        ),
                        Set.of()
                    )
                )
            )
        );
        var e = CrossProjectIndexResolutionValidator.validate(
            getStrictAllowNoIndices(),
            useProjectRouting ? "_alias:P1" : null, // a redundant project routing has no impact
            local,
            remote
        );
        assertNotNull(e);
        assertThat(e, instanceOf(IndexNotFoundException.class));
        assertThat(e.getMessage(), containsString("no such index [P1:logs*]"));
    }

    public void testFlatExpressionWithMultipleRemotesPartiallyUnauthorizedStrictIgnoreUnavailable() {
        var local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "logs",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of("logs"),
                        ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE,
                        null
                    ),
                    Set.of("P1:logs", "P2:logs")
                )
            )
        );
        var remote = Map.of(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of("logs"),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED,
                            new ElasticsearchSecurityException("Unauthorized for -*")
                        ),
                        Set.of()
                    )
                )
            ),
            "P2",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of("logs"),
                            ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS,
                            null
                        ),
                        Set.of()
                    )
                )
            )
        );

        var e = CrossProjectIndexResolutionValidator.validate(getStrictIgnoreUnavailable(), null, local, remote);
        assertThat(e, is(nullValue()));
    }

    private IndicesOptions getStrictAllowNoIndices() {
        return getIndicesOptions(true, false);
    }

    private IndicesOptions getStrictIgnoreUnavailable() {
        return getIndicesOptions(false, true);
    }

    private IndicesOptions getLenientIndicesOptions() {
        return getIndicesOptions(true, true);
    }

    private IndicesOptions getIndicesOptions(boolean ignoreUnavailable, boolean allowNoIndices) {
        return IndicesOptions.fromOptions(ignoreUnavailable, allowNoIndices, randomBoolean(), randomBoolean());
    }
}
