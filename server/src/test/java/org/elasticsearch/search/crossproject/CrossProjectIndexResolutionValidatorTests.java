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
import org.elasticsearch.action.fieldcaps.RemoteDatasetNotSupportedException;
import org.elasticsearch.action.fieldcaps.RemoteResourceNotSupportedException;
import org.elasticsearch.action.fieldcaps.RemoteViewNotSupportedException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteTransportException;
import org.junit.Before;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class CrossProjectIndexResolutionValidatorTests extends ESTestCase {

    private boolean useProjectRouting;

    @Before
    public void initProjectRouting() throws Exception {
        useProjectRouting = randomBoolean();
    }

    public void testLenientIndicesOptions() {
        // with lenient IndicesOptions we early terminate without error
        assertNull(
            CrossProjectIndexResolutionValidator.validate(getLenientIndicesOptions(), randomFrom("_alias:*", null), null, null, Map.of())
        );
    }

    public void testFlatExpressionWithStrictIgnoreUnavailableMatchingInOriginProject() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "logs",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of("logs"),
                        ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS
                    ),
                    Set.of("P1:logs")
                )
            ),
            null
        );

        // we matched resource locally thus no error
        assertNull(
            CrossProjectIndexResolutionValidator.validate(
                getStrictIgnoreUnavailable(),
                useProjectRouting ? "_alias:*" : null,  // a redundant project routing has no impact
                local,
                null,
                Map.of()
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
                        ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                    ),
                    Set.of("P1:logs")
                )
            ),
            null
        );

        var remote = Map.of(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of("logs"),
                            ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS
                        ),
                        Set.of()
                    )
                ),
                null
            )
        );

        // we matched the flat resource in a linked project thus no error
        assertNull(
            CrossProjectIndexResolutionValidator.validate(
                getStrictIgnoreUnavailable(),
                useProjectRouting ? "_alias:*" : null,  // a redundant project routing has no impact
                local,
                remote,
                Map.of()
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
                        ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                    ),
                    Set.of("P1:logs")
                )
            ),
            null
        );

        var remote = Map.of(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                        ),
                        Set.of()
                    )
                ),
                null
            )
        );
        var e = CrossProjectIndexResolutionValidator.validate(
            getStrictIgnoreUnavailable(),
            useProjectRouting ? "_alias:*" : null,  // a redundant project routing has no impact
            local,
            remote,
            Map.of()
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
                        ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                    ),
                    Set.of("P1:logs")
                )
            ),
            null
        );

        var remoteExceptions = Map.of("P1", new Exception("Unable to connect to [P1]"));

        // logs does not exist in the remote responses and indices options are strict. We expect an error.
        var e = CrossProjectIndexResolutionValidator.validate(
            getStrictIgnoreUnavailable(),
            useProjectRouting ? "_alias:*" : null,  // a redundant project routing has no impact
            local,
            Map.of(),
            remoteExceptions
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
                        ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                    ),
                    Set.of("P1:logs")
                )
            ),
            null
        );

        // logs does not exist in the remote responses and ignore_unavailable is set to true. We do not expect an error.
        var e = CrossProjectIndexResolutionValidator.validate(
            getLenientIndicesOptions(),
            useProjectRouting ? "_alias:*" : null,  // a redundant project routing has no impact
            local,
            Map.of(),
            Map.of()
        );
        assertNull(e);
    }

    public void testMissingResponseFromLinkedProjectsWithStrictAllowNoIndices() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "logs*",
                    new ResolvedIndexExpression.LocalExpressions(Set.of(), ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS),
                    Set.of("P1:logs*")
                )
            ),
            null
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
                            ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS
                        ),
                        Set.of()
                    )
                ),
                null
            )
        );

        var remoteExceptions = Map.of("P1", new Exception("Unable to connect to [P1]"));

        // Index expression is a wildcard-ed expression but the indices options are strict. We expect an error.
        var e = CrossProjectIndexResolutionValidator.validate(
            getStrictAllowNoIndices(),
            useProjectRouting ? "_alias:*" : null,  // a redundant project routing has no impact
            local,
            remote,
            remoteExceptions
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
                    new ResolvedIndexExpression.LocalExpressions(Set.of(), ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS),
                    Set.of("P1:logs*")
                )
            ),
            null
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
                            ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS
                        ),
                        Set.of()
                    )
                ),
                null
            )
        );

        // Index expression is a wildcard-ed expression but the indices options are lenient. We do not expect an error.
        var e = CrossProjectIndexResolutionValidator.validate(
            getLenientIndicesOptions(),
            useProjectRouting ? "_alias:*" : null,  // a redundant project routing has no impact
            local,
            remote,
            Map.of()
        );
        assertNull(e);
    }

    public void testMissingResponseFromLinkedProjectsForQualifiedExpressionWithStrictIgnoreUnavailable() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(new ResolvedIndexExpression("P1:logs", ResolvedIndexExpression.LocalExpressions.NONE, Set.of("P1:logs"))),
            null
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
                            ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS
                        ),
                        Set.of()
                    )
                ),
                null
            )
        );

        var remoteExceptions = Map.of("P1", new Exception("Unable to connect to [P1]"));

        // logs does not exist in the remote responses and indices options are strict. We expect an error.
        var e = CrossProjectIndexResolutionValidator.validate(
            getStrictIgnoreUnavailable(),
            useProjectRouting ? "_alias:*" : null,  // a redundant project routing has no impact
            local,
            remote,
            remoteExceptions
        );
        assertNotNull(e);
        assertThat(e, instanceOf(IndexNotFoundException.class));
        assertThat(e.getMessage(), containsString("no such index [P1:logs]"));
    }

    public void testMissingResponseFromLinkedProjectsForQualifiedExpressionWithLenientIgnoreUnavailable() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(new ResolvedIndexExpression("P1:logs", ResolvedIndexExpression.LocalExpressions.NONE, Set.of("P1:logs"))),
            null
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
                            ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS
                        ),
                        Set.of()
                    )
                ),
                null
            )
        );

        // logs does not exist in the remote responses and indices options are lenient. We do not expect an error.
        var e = CrossProjectIndexResolutionValidator.validate(
            getLenientIndicesOptions(),
            useProjectRouting ? "_alias:P1" : null,  // a redundant project routing has no impact
            local,
            remote,
            Map.of()
        );
        assertNull(e);
    }

    public void testUnauthorizedFlatExpressionWithStrictIgnoreUnavailable() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "logs",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of(),
                        ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED
                    ),
                    Set.of("P1:logs")
                )
            ),
            "authorization errors while resolving [-*]"
        );

        var remote = Map.of(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED
                        ),
                        Set.of()
                    )
                ),
                "authorization errors while resolving [logs]"
            )
        );

        var e = CrossProjectIndexResolutionValidator.validate(
            getStrictIgnoreUnavailable(),
            useProjectRouting ? "_alias:*" : null,  // a redundant project routing has no impact
            local,
            remote,
            Map.of()
        );
        assertNotNull(e);
        assertThat(e.getMessage(), equalTo("authorization errors while resolving [logs]"));
    }

    public void testUnauthorizedFlatExpressionWithStrictIgnoreUnavailableAndProjectRouting() {
        final String projectRouting = "_alias:P1";
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "logs",
                    ResolvedIndexExpression.LocalExpressions.NONE, // no local resolution since it is excluded by project routing
                    Set.of("P1:logs")
                )
            ),
            null
        );

        var remote = Map.of(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED
                        ),
                        Set.of()
                    )
                ),
                "authorization errors while resolving [-*]"
            )
        );

        var e = CrossProjectIndexResolutionValidator.validate(getStrictIgnoreUnavailable(), projectRouting, local, remote, Map.of());
        assertNotNull(e);
        assertThat(e.getMessage(), equalTo("authorization errors while resolving [P1:logs]"));
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
            ),
            null
        );

        var remote = Map.of(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                        ),
                        Set.of()
                    )
                ),
                null
            )
        );

        var e = CrossProjectIndexResolutionValidator.validate(getStrictIgnoreUnavailable(), projectRouting, local, remote, Map.of());
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
                        ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS
                    ),
                    Set.of()
                )
            ),
            null
        );

        // we matched locally thus no error
        assertNull(
            CrossProjectIndexResolutionValidator.validate(
                getStrictIgnoreUnavailable(),
                useProjectRouting ? "_alias:_origin" : null, // a redundant project routing has no impact
                local,
                null,
                Map.of()
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
                        ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                    ),
                    Set.of()
                )
            ),
            null
        );

        var e = CrossProjectIndexResolutionValidator.validate(
            getStrictIgnoreUnavailable(),
            useProjectRouting ? "_alias:_origin" : null, // a redundant project routing has no impact
            local,
            null,
            Map.of()
        );
        assertNotNull(e);
        assertThat(e, instanceOf(IndexNotFoundException.class));
        assertThat(e.getMessage(), containsString("no such index [" + original + "]"));
    }

    public void testQualifiedExpressionWithStrictIgnoreUnavailableMatchingInLinkedProject() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(new ResolvedIndexExpression("P1:logs", ResolvedIndexExpression.LocalExpressions.NONE, Set.of("P1:logs"))),
            null
        );

        var remote = Map.of(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of("logs"),
                            ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS
                        ),
                        Set.of()
                    )
                ),
                null
            )
        );

        // we matched the flat resource in a linked project thus no error
        assertNull(
            CrossProjectIndexResolutionValidator.validate(
                getStrictIgnoreUnavailable(),
                useProjectRouting ? "_alias:P1" : null, // a redundant project routing has no impact
                local,
                remote,
                Map.of()
            )
        );
    }

    public void testMissingQualifiedExpressionWithStrictIgnoreUnavailable() {
        final String original = "P1:logs";
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    original,
                    new ResolvedIndexExpression.LocalExpressions(Set.of(), ResolvedIndexExpression.LocalIndexResolutionResult.NONE),
                    Set.of("P1:logs")
                )
            ),
            null
        );

        var remote = Map.of(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                        ),
                        Set.of()
                    )
                ),
                null
            )
        );

        var e = CrossProjectIndexResolutionValidator.validate(
            getStrictIgnoreUnavailable(),
            useProjectRouting ? "_alias:P1" : null, // a redundant project routing has no impact
            local,
            remote,
            Map.of()
        );
        assertNotNull(e);
        assertThat(e, instanceOf(IndexNotFoundException.class));
        assertThat(e.getMessage(), containsString("no such index [P1:logs]"));
    }

    public void testUnauthorizedQualifiedExpressionWithStrictIgnoreUnavailable() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(new ResolvedIndexExpression("P1:logs", ResolvedIndexExpression.LocalExpressions.NONE, Set.of("P1:logs"))),
            null
        );

        var remote = Map.of(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED
                        ),
                        Set.of()
                    )
                ),
                "action is unauthorized for indices [-*]"
            )
        );

        var e = CrossProjectIndexResolutionValidator.validate(
            getStrictIgnoreUnavailable(),
            useProjectRouting ? "_alias:P1" : null, // a redundant project routing has no impact
            local,
            remote,
            Map.of()
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
                        ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS
                    ),
                    Set.of("P1:logs")
                )
            ),
            null
        );

        // we matched resource locally thus no error
        assertNull(CrossProjectIndexResolutionValidator.validate(getStrictAllowNoIndices(), null, local, null, Map.of()));
    }

    public void testStrictAllowNoIndicesFoundEmptyResultsOnOriginAndLinked() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "shared-index-missing*",
                    new ResolvedIndexExpression.LocalExpressions(Set.of(), ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS),
                    Set.of("P1:shared-index-missing*")
                )
            ),
            null
        );

        var remote = Map.of(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "shared-index-missing*",
                        new ResolvedIndexExpression.LocalExpressions(Set.of(), ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS),
                        Set.of()
                    )
                ),
                null
            )
        );

        ElasticsearchException ex = CrossProjectIndexResolutionValidator.validate(
            getIndicesOptions(false, false),
            null,
            local,
            remote,
            Map.of()
        );
        assertNotNull(ex);
        assertThat(ex, instanceOf(IndexNotFoundException.class));
        assertThat(ex.getMessage(), containsString("no such index [shared-index-missing*]"));
    }

    public void testMissingConcreteIndicesWithIgnoreUnavailableAndStrictAllowNoIndices() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "logs",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of("logs"),
                        ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                    ),
                    Set.of()
                )
            ),
            null
        );

        var e = CrossProjectIndexResolutionValidator.validate(getStrictAllowNoIndices(), null, local, Map.of(), Map.of());
        assertNotNull(e);
        assertThat(e, instanceOf(IndexNotFoundException.class));
        assertThat(e.getMessage(), containsString("no such index [logs]"));
    }

    public void testMultipleMissingConcreteIndicesWithIgnoreUnavailableAndStrictAllowNoIndices() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "logs",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of("logs"),
                        ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                    ),
                    Set.of()
                ),
                new ResolvedIndexExpression(
                    "metrics",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of("metrics"),
                        ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                    ),
                    Set.of()
                )
            ),
            null
        );

        var e = CrossProjectIndexResolutionValidator.validate(getStrictAllowNoIndices(), null, local, Map.of(), Map.of());
        assertNotNull(e);
        assertThat(e, instanceOf(IndexNotFoundException.class));
        assertThat(e.getMessage(), containsString("no such index [logs,metrics]"));
    }

    public void testMissingConcreteIndicesWithLinkedProjectAndStrictAllowNoIndices() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "logs",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of("logs"),
                        ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                    ),
                    Set.of("P1:logs")
                )
            ),
            null
        );

        var remote = Map.of(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of("logs"),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                        ),
                        Set.of()
                    )
                ),
                null
            )
        );

        var e = CrossProjectIndexResolutionValidator.validate(getStrictAllowNoIndices(), null, local, remote, Map.of());
        assertNotNull(e);
        assertThat(e, instanceOf(IndexNotFoundException.class));
        assertThat(e.getMessage(), containsString("no such index [logs]"));
    }

    public void testMixedExistingAndMissingConcreteIndicesWithStrictAllowNoIndices() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "logs",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of("logs"),
                        ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS
                    ),
                    Set.of()
                ),
                new ResolvedIndexExpression(
                    "missing",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of("missing"),
                        ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                    ),
                    Set.of()
                )
            ),
            null
        );

        // One index exists so the overall result is non-empty — no error expected
        assertNull(CrossProjectIndexResolutionValidator.validate(getStrictAllowNoIndices(), null, local, Map.of(), Map.of()));
    }

    public void testFlatExpressionWithStrictAllowNoIndicesMatchingInLinkedProject() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "logs*",
                    new ResolvedIndexExpression.LocalExpressions(Set.of(), ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS),
                    Set.of("P1:logs*")
                )
            ),
            null
        );

        var remote = Map.of(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs*",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of("logs-es"),
                            ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS
                        ),
                        Set.of()
                    )
                ),
                null
            )
        );

        // we matched the flat resource in a linked project thus no error
        assertNull(CrossProjectIndexResolutionValidator.validate(getStrictAllowNoIndices(), null, local, remote, Map.of()));
    }

    public void testMissingFlatExpressionWithStrictAllowNoIndices() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "logs*",
                    new ResolvedIndexExpression.LocalExpressions(Set.of(), ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS),
                    Set.of("P1:logs*")
                )
            ),
            null
        );

        var remote = Map.of(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs*",
                        new ResolvedIndexExpression.LocalExpressions(Set.of(), ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS),
                        Set.of()
                    )
                ),
                null
            )
        );

        var e = CrossProjectIndexResolutionValidator.validate(getStrictAllowNoIndices(), null, local, remote, Map.of());
        assertNotNull(e);
        assertThat(e, instanceOf(IndexNotFoundException.class));
        assertThat(e.getMessage(), containsString("no such index [logs*]"));
    }

    public void testUnauthorizedFlatExpressionWithStrictAllowNoIndices() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "logs*",
                    new ResolvedIndexExpression.LocalExpressions(Set.of(), ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS),
                    Set.of("P1:logs*")
                )
            ),
            null
        );

        var remote = Map.of(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs*",
                        new ResolvedIndexExpression.LocalExpressions(Set.of(), ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS),
                        Set.of()
                    )
                ),
                null
            )
        );

        var e = CrossProjectIndexResolutionValidator.validate(getStrictAllowNoIndices(), null, local, remote, Map.of());
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
                        ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS
                    ),
                    Set.of()
                )
            ),
            null
        );

        // we matched locally thus no error
        assertNull(
            CrossProjectIndexResolutionValidator.validate(
                getStrictAllowNoIndices(),
                useProjectRouting ? "_alias:_origin" : null, // a redundant project routing has no impact
                local,
                null,
                Map.of()
            )
        );
    }

    public void testQualifiedOriginExpressionWithStrictAllowNoIndicesNotMatching() {
        final String original = "_origin:logs*";
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    original,
                    new ResolvedIndexExpression.LocalExpressions(Set.of(), ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS),
                    Set.of()
                )
            ),
            null
        );
        var e = CrossProjectIndexResolutionValidator.validate(
            getStrictAllowNoIndices(),
            useProjectRouting ? "_alias:_origin" : null, // a redundant project routing has no impact
            local,
            null,
            Map.of()
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
                            ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS
                        ),
                        Set.of()
                    )
                ),
                null
            );
            assertNull(
                CrossProjectIndexResolutionValidator.validate(
                    getIndicesOptions(randomBoolean(), randomBoolean()),
                    useProjectRouting ? "_alias:_origin" : null, // a redundant project routing has no impact
                    local,
                    Map.of(),
                    Map.of()
                )
            );
        }
    }

    public void testQualifiedExpressionWithStrictAllowNoIndicesMatchingInLinkedProject() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(new ResolvedIndexExpression("P1:logs*", ResolvedIndexExpression.LocalExpressions.NONE, Set.of("P1:logs*"))),
            null
        );

        var remote = Map.of(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs*",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of("logs-es"),
                            ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS
                        ),
                        Set.of()
                    )
                ),
                null
            )
        );

        // we matched the flat resource in a linked project thus no error
        assertNull(
            CrossProjectIndexResolutionValidator.validate(
                getStrictAllowNoIndices(),
                useProjectRouting ? "_alias:P1" : null,  // a redundant project routing has no impact
                local,
                remote,
                Map.of()
            )
        );
    }

    public void testMissingQualifiedExpressionWithStrictAllowNoIndices() {
        final String original = "P1:logs*";
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    original,
                    new ResolvedIndexExpression.LocalExpressions(Set.of(), ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS),
                    Set.of("P1:logs*")
                )
            ),
            null
        );

        var remote = Map.of(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs*",
                        new ResolvedIndexExpression.LocalExpressions(Set.of(), ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS),
                        Set.of()
                    )
                ),
                null
            )
        );

        var e = CrossProjectIndexResolutionValidator.validate(
            getStrictAllowNoIndices(),
            useProjectRouting ? "_alias:P1" : null, // a redundant project routing has no impact
            local,
            remote,
            Map.of()
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
                    new ResolvedIndexExpression.LocalExpressions(Set.of(), ResolvedIndexExpression.LocalIndexResolutionResult.NONE),
                    Set.of("P1:logs*")
                )
            ),
            null
        );

        var remote = Map.of(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs*",
                        new ResolvedIndexExpression.LocalExpressions(Set.of(), ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS),
                        Set.of()
                    )
                ),
                null
            )
        );
        var e = CrossProjectIndexResolutionValidator.validate(
            getStrictAllowNoIndices(),
            useProjectRouting ? "_alias:P1" : null, // a redundant project routing has no impact
            local,
            remote,
            Map.of()
        );
        assertNotNull(e);
        assertThat(e, instanceOf(IndexNotFoundException.class));
        assertThat(e.getMessage(), containsString("no such index [P1:logs*]"));
    }

    public void testUnqualifiedExpressionSuccessWhenFoundOnAnyProject() {
        var local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "logs",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of(),
                        ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                    ),
                    Set.of("P1:logs", "P2:logs")
                )
            ),
            null
        );
        var remote = Map.of(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED
                        ),
                        Set.of()
                    )
                ),
                "Unauthorized for -*"
            ),
            "P2",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(Set.of(), ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS),
                        Set.of()
                    )
                ),
                null
            )
        );

        var e = CrossProjectIndexResolutionValidator.validate(getStrictIgnoreUnavailable(), null, local, remote, Map.of());
        assertThat(e, is(nullValue()));
    }

    public void testReport403Over404() {
        var local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "logs",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of("logs"),
                        ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                    ),
                    Set.of("P1:logs", "P2:logs")
                )
            ),
            null
        );
        var remote = Map.of(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of("logs"),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED
                        ),
                        Set.of()
                    )
                ),
                "Unauthorized for -*"
            )
        );

        var remoteExceptions = Map.of("P2", new Exception("Unable to connect to [P2]"));
        var e = CrossProjectIndexResolutionValidator.validate(getStrictIgnoreUnavailable(), null, local, remote, remoteExceptions);
        assertThat(e, instanceOf(ElasticsearchSecurityException.class));
        assertThat(e.getMessage(), containsString("P1:logs"));
    }

    public void testUnqualifiedIndexExpressionShouldReportFirst403() {
        var local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "metrics",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of(),
                        ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                    ),
                    Set.of("P1:metrics", "P2:metrics")
                ),
                new ResolvedIndexExpression(
                    "logs",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of(),
                        ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                    ),
                    Set.of("P1:logs", "P2:logs")
                )
            ),
            null
        );
        var remote = new LinkedHashMap<String, ResolvedIndexExpressions>();
        remote.put(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "metrics",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED
                        ),
                        Set.of()
                    ),
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED
                        ),
                        Set.of()
                    )
                ),
                "Unauthorized for -*"
            )
        );
        remote.put(
            "P2",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "metrics",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED
                        ),
                        Set.of()
                    ),
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(Set.of(), ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS),
                        Set.of()
                    )
                ),
                "Unauthorized for -*"
            )
        );

        var e = CrossProjectIndexResolutionValidator.validate(getStrictIgnoreUnavailable(), null, local, remote, Map.of());
        assertThat(e, is(notNullValue()));
        assertThat(e.getMessage(), equalTo("Unauthorized for P1:metrics"));
        assertThat(e.getSuppressed(), emptyArray());
    }

    public void testQualifiedExpressionShouldReport403FromAllProjects() {
        var local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "*:metrics",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of(),
                        ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                    ),
                    Set.of("P1:metrics", "P2:metrics")
                ),
                new ResolvedIndexExpression(
                    "*:logs",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of(),
                        ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                    ),
                    Set.of("P1:logs", "P2:logs")
                )
            ),
            null
        );
        var remote = new LinkedHashMap<String, ResolvedIndexExpressions>();
        remote.put(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "metrics",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED
                        ),
                        Set.of()
                    ),
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED
                        ),
                        Set.of()
                    )
                ),
                "Unauthorized for -*"
            )
        );
        remote.put(
            "P2",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "metrics",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED
                        ),
                        Set.of()
                    ),
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(Set.of(), ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS),
                        Set.of()
                    )
                ),
                "Unauthorized for -*"
            )
        );

        var e = CrossProjectIndexResolutionValidator.validate(getStrictIgnoreUnavailable(), null, local, remote, Map.of());
        assertThat(e, is(notNullValue()));
        assertThat(e.getMessage(), equalTo("Unauthorized for P1:metrics,P1:logs"));
        assertThat(e.getSuppressed(), arrayWithSize(1));
        assertThat(e.getSuppressed()[0].getMessage(), equalTo("Unauthorized for P2:metrics"));
    }

    public void testShouldReportFirst404ExceptionWhenNo403() {
        var local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "metrics",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of(),
                        ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                    ),
                    Set.of("P1:metrics", "P2:metrics")
                ),
                new ResolvedIndexExpression(
                    "logs",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of(),
                        ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                    ),
                    Set.of("P1:logs", "P2:logs")
                )
            ),
            null
        );
        var remote = new LinkedHashMap<String, ResolvedIndexExpressions>();
        remote.put(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "metrics",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                        ),
                        Set.of()
                    ),
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                        ),
                        Set.of()
                    )
                ),
                null
            )
        );
        remote.put(
            "P2",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "metrics",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                        ),
                        Set.of()
                    ),
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                        ),
                        Set.of()
                    )
                ),
                null
            )
        );

        var e = CrossProjectIndexResolutionValidator.validate(getStrictIgnoreUnavailable(), null, local, remote, Map.of());
        assertThat(e, is(notNullValue()));
        assertThat(e.getMessage(), equalTo("no such index [metrics]"));
        assertThat(e.getSuppressed(), arrayWithSize(1));
        assertThat(e.getSuppressed()[0].getMessage(), equalTo("no such index [logs]"));
    }

    public void testShouldReportFirstRemote404WhenNo403AndLocalProjectIsExcludedForQualifiedExpression() {
        var local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression("*:metrics", ResolvedIndexExpression.LocalExpressions.NONE, Set.of("P1:metrics", "P2:metrics")),
                new ResolvedIndexExpression("*:logs", ResolvedIndexExpression.LocalExpressions.NONE, Set.of("P1:logs", "P2:logs"))
            ),
            null
        );
        var remote = new LinkedHashMap<String, ResolvedIndexExpressions>();
        remote.put(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "metrics",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                        ),
                        Set.of()
                    ),
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                        ),
                        Set.of()
                    )
                ),
                null
            )
        );
        remote.put(
            "P2",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "metrics",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                        ),
                        Set.of()
                    ),
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                        ),
                        Set.of()
                    )
                ),
                null
            )
        );

        var e = CrossProjectIndexResolutionValidator.validate(getStrictIgnoreUnavailable(), null, local, remote, Map.of());
        assertThat(e, is(notNullValue()));
        assertThat(e.getMessage(), equalTo("no such index [P1:metrics]"));
        assertThat(e.getSuppressed(), arrayWithSize(3));
    }

    public void testShouldReportFirstRemote404sWhenNo403AndLocalProjectIsExcludedForUnqualifiedExpressions() {
        var local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression("metrics", ResolvedIndexExpression.LocalExpressions.NONE, Set.of("P1:metrics", "P2:metrics")),
                new ResolvedIndexExpression("logs", ResolvedIndexExpression.LocalExpressions.NONE, Set.of("P1:logs", "P2:logs"))
            ),
            null
        );
        var remote = new LinkedHashMap<String, ResolvedIndexExpressions>();
        remote.put(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "metrics",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                        ),
                        Set.of()
                    ),
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                        ),
                        Set.of()
                    )
                ),
                null
            )
        );
        remote.put(
            "P2",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "metrics",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                        ),
                        Set.of()
                    ),
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                        ),
                        Set.of()
                    )
                ),
                null
            )
        );

        var e = CrossProjectIndexResolutionValidator.validate(getStrictIgnoreUnavailable(), null, local, remote, Map.of());
        assertThat(e, is(notNullValue()));
        assertThat(e.getMessage(), equalTo("no such index [P1:metrics]"));
        assertThat(e.getSuppressed(), arrayWithSize(1));
        assertThat(e.getSuppressed()[0].getMessage(), equalTo("no such index [P1:logs]"));
    }

    public void testResolvedIndexExpressionsAreCopiedOntoNewSearchRequest() {
        ResolvedIndexExpressions expr = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "logs",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of("logs"),
                        ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS
                    ),
                    Set.of("P1:logs")
                )
            ),
            null
        );

        String projectRouting = "_alias:_origin";
        SearchRequest original = new SearchRequest("logs");
        original.setResolvedIndexExpressions(expr);
        original.setProjectRouting(projectRouting);

        /*
         * When a new SearchRequest object is created from an existing one, we should copy over the previously
         * resolved expressions since the new object will not go through the Security Action Filter.
         */
        SearchRequest rewritten = new SearchRequest(original);
        assertThat(rewritten.getResolvedIndexExpressions(), equalTo(expr));
        assertThat(rewritten.getProjectRouting(), equalTo(projectRouting));
    }

    public void testValidationWorksWithExclusions() {
        {
            // Exclusion by itself
            final var resolvedExclusion = randomFrom(
                new ResolvedIndexExpression("-logs", ResolvedIndexExpression.LocalExpressions.NONE, Set.of("P1:-logs")),
                new ResolvedIndexExpression("-logs*", ResolvedIndexExpression.LocalExpressions.NONE, Set.of("P1:-logs*"))
            );
            final var local = new ResolvedIndexExpressions(List.of(resolvedExclusion), null);
            var remote = Map.of("P1", new ResolvedIndexExpressions(List.of(), null));

            assertNull(
                CrossProjectIndexResolutionValidator.validate(
                    getStrictIgnoreUnavailable(),
                    useProjectRouting ? "_alias:*" : null,  // a redundant project routing has no impact
                    local,
                    remote,
                    Map.of()
                )
            );
        }

        {
            // Exclusion with includes
            final var resolvedExclusion = randomFrom(
                new ResolvedIndexExpression("-logs*", ResolvedIndexExpression.LocalExpressions.NONE, Set.of("P1:-logs*")),
                new ResolvedIndexExpression("-P1:logs*", ResolvedIndexExpression.LocalExpressions.NONE, Set.of("-P1:logs*")),
                new ResolvedIndexExpression("P1:-logs*", ResolvedIndexExpression.LocalExpressions.NONE, Set.of("P1:-logs*"))
            );

            final var local = new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "*",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of("metrics"),
                            ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS
                        ),
                        Set.of("P1:*")
                    ),
                    resolvedExclusion
                ),
                null
            );
            var remote = Map.of(
                "P1",
                new ResolvedIndexExpressions(
                    List.of(
                        new ResolvedIndexExpression(
                            "*",
                            new ResolvedIndexExpression.LocalExpressions(
                                Set.of("remote-metrics"),
                                ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS
                            ),
                            Set.of()
                        )
                    ),
                    null
                )
            );

            assertNull(
                CrossProjectIndexResolutionValidator.validate(
                    getStrictAllowNoIndices(),
                    useProjectRouting ? "_alias:*" : null,  // a redundant project routing has no impact
                    local,
                    remote,
                    Map.of()
                )
            );
        }
    }

    public void testSingleExclusionExpressionWithStrictAllowNoIndices() {
        // Exclusion by itself
        var expression = randomBoolean() ? "-logs" : "-logs*";
        final var resolvedExclusion = randomFrom(
            new ResolvedIndexExpression(expression, ResolvedIndexExpression.LocalExpressions.NONE, Set.of("P1:" + expression)),
            new ResolvedIndexExpression(expression, ResolvedIndexExpression.LocalExpressions.NONE, Set.of("P1:" + expression))
        );
        final var local = new ResolvedIndexExpressions(List.of(resolvedExclusion), null);
        var remote = Map.of("P1", new ResolvedIndexExpressions(List.of(), null));

        var ex = CrossProjectIndexResolutionValidator.validate(
            getStrictAllowNoIndices(),
            useProjectRouting ? "_alias:*" : null,  // a redundant project routing has no impact
            local,
            remote,
            Map.of()
        );
        assertNotNull(ex);
        assertThat(ex.getMessage(), equalTo("no such index [" + expression + "]"));
    }

    public void testMultipleResolvingToNoIndicesWithStrictAllowNoIndices() {
        // given an index expression "shared-index-1,-shared-index-1,shared-index-2,-shared-index-2", it resolves as the below
        var local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression("-shared-index-1", ResolvedIndexExpression.LocalExpressions.NONE, Set.of("P1:-shared-index-1")),
                new ResolvedIndexExpression("-shared-index-2", ResolvedIndexExpression.LocalExpressions.NONE, Set.of("P1:-shared-index-2"))
            ),
            null
        );
        var remote = Map.of("P1", new ResolvedIndexExpressions(List.of(), null));

        var ex = CrossProjectIndexResolutionValidator.validate(
            getStrictAllowNoIndices(),
            useProjectRouting ? "_alias:*" : null,  // a redundant project routing has no impact
            local,
            remote,
            Map.of()
        );
        assertNotNull(ex);
        assertThat(ex.getMessage(), equalTo("no such index [-shared-index-1,-shared-index-2]"));
    }

    public void testExplicitProjectInclusionWithProjectExclusionExpressionAndLenientAllowNoIndices() {
        var local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "*:logs",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of("logs"),
                        ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS
                    ),
                    Set.of("P1:logs")
                ),
                new ResolvedIndexExpression(
                    randomFrom("P1:-*", "P*:-*"),
                    new ResolvedIndexExpression.LocalExpressions(Set.of(), ResolvedIndexExpression.LocalIndexResolutionResult.NONE),
                    Set.of()
                )
            ),
            null
        );

        var remote = Map.of("P1", new ResolvedIndexExpressions(List.of(), null));

        assertNull(
            CrossProjectIndexResolutionValidator.validate(
                getLenientIndicesOptions(),
                useProjectRouting ? "_alias:*" : null,  // a redundant project routing has no impact
                local,
                remote,
                Map.of()
            )
        );
    }

    public void testExplicitProjectInclusionWithProjectExclusionExpressionAndStrictAllowNoIndices() {
        var local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "*:logs",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of("logs"),
                        ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS
                    ),
                    Set.of("P1:logs")
                ),
                new ResolvedIndexExpression(
                    randomFrom("P1:-*", "P*:-*", "P1:-logs"),
                    new ResolvedIndexExpression.LocalExpressions(Set.of(), ResolvedIndexExpression.LocalIndexResolutionResult.NONE),
                    Set.of("P1:-*")
                )
            ),
            null
        );

        var remote = Map.of("P1", new ResolvedIndexExpressions(List.of(), null));

        var e = CrossProjectIndexResolutionValidator.validate(
            getStrictAllowNoIndices(),
            useProjectRouting ? "_alias:*" : null,  // a redundant project routing has no impact
            local,
            remote,
            Map.of()
        );
        assertNotNull(e);
        assertThat(e.getMessage(), equalTo("no such index [P1:logs]"));
    }

    public void testRemoteViewNotSupportedExceptionFromLinkedProject() {
        ResolvedIndexExpressions local = flatExpressionWithRemoteFanout("my-view", "P1:my-view");
        Map<String, Exception> remoteExceptions = Map.of(
            "P1",
            new RemoteTransportException("test failure", new RemoteViewNotSupportedException(List.of("P1:my-view")))
        );

        var e = CrossProjectIndexResolutionValidator.validate(
            randomBoolean() ? getStrictIgnoreUnavailable() : getLenientIndicesOptions(),
            useProjectRouting ? "_alias:*" : null,
            local,
            Map.of(),
            remoteExceptions
        );
        assertThat(e, instanceOf(RemoteResourceNotSupportedException.class));
        assertThat(
            e.getMessage(),
            equalTo(
                "ES|QL queries with remote views are not supported. Matched [P1:my-view]."
                    + " Remove them from the query pattern or exclude them with [P1:-my-view] if matched by a wildcard."
            )
        );
        assertThat(e.getMetadata("es.esql.view.names"), equalTo(List.of("P1:my-view")));
        assertNull(e.getMetadata("es.esql.dataset.names"));
    }

    public void testRemoteDatasetNotSupportedExceptionFromLinkedProject() {
        ResolvedIndexExpressions local = flatExpressionWithRemoteFanout("my-dataset", "P1:my-dataset");
        Map<String, Exception> remoteExceptions = Map.of(
            "P1",
            new RemoteTransportException("test failure", new RemoteDatasetNotSupportedException(List.of("P1:my-dataset")))
        );

        var e = CrossProjectIndexResolutionValidator.validate(
            randomBoolean() ? getStrictIgnoreUnavailable() : getLenientIndicesOptions(),
            useProjectRouting ? "_alias:*" : null,
            local,
            Map.of(),
            remoteExceptions
        );
        assertThat(e, instanceOf(RemoteResourceNotSupportedException.class));
        assertThat(
            e.getMessage(),
            equalTo(
                "ES|QL queries with remote datasets are not supported. Matched [P1:my-dataset]."
                    + " Remove them from the query pattern or exclude them with [P1:-my-dataset] if matched by a wildcard."
            )
        );
        assertNull(e.getMetadata("es.esql.view.names"));
        assertThat(e.getMetadata("es.esql.dataset.names"), equalTo(List.of("P1:my-dataset")));
    }

    public void testRemoteViewAndDatasetNotSupportedExceptionAggregatedAcrossLinkedProjects() {
        ResolvedIndexExpressions local = flatExpressionWithRemoteFanout("logs-*", "P1:logs-*", "P2:logs-*");
        Map<String, Exception> remoteExceptions = Map.of(
            "P1",
            new RemoteTransportException("test failure", new RemoteViewNotSupportedException(List.of("P1:my-view"))),
            "P2",
            new RemoteTransportException("test failure", new RemoteDatasetNotSupportedException(List.of("P2:my-dataset")))
        );

        var e = CrossProjectIndexResolutionValidator.validate(
            randomBoolean() ? getStrictIgnoreUnavailable() : getLenientIndicesOptions(),
            useProjectRouting ? "_alias:*" : null,
            local,
            Map.of(),
            remoteExceptions
        );
        assertThat(e, instanceOf(RemoteResourceNotSupportedException.class));
        assertThat(
            e.getMessage(),
            equalTo(
                "ES|QL queries with remote views and datasets are not supported. Matched views [P1:my-view], datasets [P2:my-dataset]."
                    + " Remove them from the query pattern or exclude them with [P1:-my-view,P2:-my-dataset] if matched by a wildcard."
            )
        );
        assertThat(e.getMetadata("es.esql.view.names"), equalTo(List.of("P1:my-view")));
        assertThat(e.getMetadata("es.esql.dataset.names"), equalTo(List.of("P2:my-dataset")));
    }

    public void testRemoteResourceNotSupportedExceptionAggregatesMultipleViewsAcrossLinkedProjects() {
        ResolvedIndexExpressions local = flatExpressionWithRemoteFanout("logs-*", "P1:logs-*", "P2:logs-*");
        Map<String, Exception> remoteExceptions = Map.of(
            "P1",
            new RemoteTransportException("test failure", new RemoteViewNotSupportedException(List.of("P1:view-1"))),
            "P2",
            new RemoteTransportException("test failure", new RemoteViewNotSupportedException(List.of("P2:view-2")))
        );

        var e = CrossProjectIndexResolutionValidator.validate(
            randomBoolean() ? getStrictIgnoreUnavailable() : getLenientIndicesOptions(),
            useProjectRouting ? "_alias:*" : null,
            local,
            Map.of(),
            remoteExceptions
        );
        assertThat(e, instanceOf(RemoteResourceNotSupportedException.class));
        assertThat(e.getMessage(), containsString("ES|QL queries with remote views are not supported."));
        assertThat(e.getMetadata("es.esql.view.names"), containsInAnyOrder("P1:view-1", "P2:view-2"));
        assertNull(e.getMetadata("es.esql.dataset.names"));
    }

    public void testRemoteResourceNotSupportedExceptionFromCombinedRemoteException() {
        ResolvedIndexExpressions local = flatExpressionWithRemoteFanout("logs-*", "P1:logs-*");
        var resourceEx = new RemoteResourceNotSupportedException(List.of("P1:view-1", "P1:view-2"), List.of("P1:dataset-1"));
        Map<String, Exception> remoteExceptions = Map.of("P1", new RemoteTransportException("test failure", resourceEx));

        var e = CrossProjectIndexResolutionValidator.validate(
            randomBoolean() ? getStrictIgnoreUnavailable() : getLenientIndicesOptions(),
            useProjectRouting ? "_alias:P1" : null,
            local,
            Map.of(),
            remoteExceptions
        );
        assertThat(e, instanceOf(RemoteResourceNotSupportedException.class));
        assertThat(e.getMetadata("es.esql.view.names"), equalTo(List.of("P1:view-1", "P1:view-2")));
        assertThat(e.getMetadata("es.esql.dataset.names"), equalTo(List.of("P1:dataset-1")));
    }

    public void testWildcardClusterAliasConcreteIndex() {

        // local index not found with cluster alias pattern
        var localNotFound = CrossProjectIndexResolutionValidator.validate(
            getStrictIgnoreUnavailable(),
            null,
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "*:logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of("logs"),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                        ),
                        Set.of("linked-1:logs")
                    )
                ),
                null
            ),
            Map.of(
                "linked-1",
                new ResolvedIndexExpressions(
                    List.of(
                        new ResolvedIndexExpression(
                            "logs",
                            new ResolvedIndexExpression.LocalExpressions(
                                Set.of("logs"),
                                ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS
                            ),
                            Set.of()
                        )
                    ),
                    null
                )
            ),
            Map.of()
        );
        assertNotNull(localNotFound);
        assertThat(localNotFound.getMessage(), equalTo("no such index [_origin:logs]"));

        // remote index not found with cluster alias pattern
        var remoteNotFound = CrossProjectIndexResolutionValidator.validate(
            getStrictIgnoreUnavailable(),
            null,
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "*:logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of("logs"),
                            ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS
                        ),
                        Set.of("linked-1:logs")
                    )
                ),
                null
            ),
            Map.of(
                "linked-1",
                new ResolvedIndexExpressions(
                    List.of(
                        new ResolvedIndexExpression(
                            "logs",
                            new ResolvedIndexExpression.LocalExpressions(
                                Set.of("logs"),
                                ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                            ),
                            Set.of()
                        )
                    ),
                    null
                )
            ),
            Map.of()
        );
        assertNotNull(remoteNotFound);
        assertThat(remoteNotFound.getMessage(), equalTo("no such index [linked-1:logs]"));
    }

    public void testRemote403ReportedOverLocalAndRemote404() {
        var local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "logs",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of(),
                        ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                    ),
                    Set.of("P1:logs", "P2:logs")
                )
            ),
            null
        );
        var remote = new LinkedHashMap<String, ResolvedIndexExpressions>();
        remote.put(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                        ),
                        Set.of()
                    )
                ),
                null
            )
        );
        remote.put(
            "P2",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED
                        ),
                        Set.of()
                    )
                ),
                "authorization errors while resolving [-*]"
            )
        );

        var e = CrossProjectIndexResolutionValidator.validate(getStrictIgnoreUnavailable(), null, local, remote, Map.of());
        assertThat(e, is(notNullValue()));
        assertThat(e.getMessage(), equalTo("authorization errors while resolving [P2:logs]"));
        assertThat(e.getSuppressed(), emptyArray());
    }

    public void testRemote403ReportedOverRemote404() {
        var local = new ResolvedIndexExpressions(
            List.of(new ResolvedIndexExpression("logs", ResolvedIndexExpression.LocalExpressions.NONE, Set.of("P1:logs", "P2:logs"))),
            null
        );
        var remote = new LinkedHashMap<String, ResolvedIndexExpressions>();
        remote.put(
            "P1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
                        ),
                        Set.of()
                    )
                ),
                null
            )
        );
        remote.put(
            "P2",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(
                            Set.of(),
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED
                        ),
                        Set.of()
                    )
                ),
                "authorization errors while resolving [-*]"
            )
        );

        var e = CrossProjectIndexResolutionValidator.validate(getStrictIgnoreUnavailable(), null, local, remote, Map.of());
        assertThat(e, is(notNullValue()));
        assertThat(e.getMessage(), equalTo("authorization errors while resolving [P2:logs]"));
        assertThat(e.getSuppressed(), emptyArray());
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

    private static ResolvedIndexExpressions flatExpressionWithRemoteFanout(String expression, String... remoteExpressions) {
        var resolvedLocally = randomFrom(
            new ResolvedIndexExpression.LocalExpressions(
                Set.of(),
                ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE
            ),
            new ResolvedIndexExpression.LocalExpressions(
                Set.of(),
                ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED
            ),
            new ResolvedIndexExpression.LocalExpressions(Set.of(expression), ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS)
        );
        return new ResolvedIndexExpressions(
            List.of(new ResolvedIndexExpression(expression, resolvedLocally, Set.of(remoteExpressions))),
            "authorization errors while resolving [-*]"
        );
    }
}
