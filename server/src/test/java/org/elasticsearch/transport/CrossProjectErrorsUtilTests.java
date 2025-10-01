/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ResolvedIndexExpression;
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;

public class CrossProjectErrorsUtilTests extends ESTestCase {
    CrossProjectErrorsUtil crossProjectErrorsUtil = new CrossProjectErrorsUtil();

    public void testLenientIndicesOptions() {
        // with lenient IndicesOptions we early terminate without error
        crossProjectErrorsUtil.crossProjectFanoutErrorHandling(getLenientIndicesOptions(), null, null);
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
        crossProjectErrorsUtil.crossProjectFanoutErrorHandling(getStrictIgnoreUnavailable(), local, null);
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
        crossProjectErrorsUtil.crossProjectFanoutErrorHandling(getStrictIgnoreUnavailable(), local, remote);
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

        expectThrows(
            IndexNotFoundException.class,
            containsString("no such index [P1:logs]"),
            () -> crossProjectErrorsUtil.crossProjectFanoutErrorHandling(getStrictIgnoreUnavailable(), local, remote)
        );
    }

    public void testUnauthorizedFlatExpressionWithStrictIgnoreUnavailable() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "logs",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of(),
                        ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED,
                        new ElasticsearchException("logs")
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
                            new ElasticsearchException("logs")
                        ),
                        Set.of()
                    )
                )
            )
        );

        expectThrows(
            ElasticsearchSecurityException.class,
            containsString("authorization errors while resolving [P1:logs]"),
            () -> crossProjectErrorsUtil.crossProjectFanoutErrorHandling(getStrictIgnoreUnavailable(), local, remote)
        );
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
        crossProjectErrorsUtil.crossProjectFanoutErrorHandling(getStrictIgnoreUnavailable(), local, null);
    }

    public void testQualifiedOriginExpressionWithStrictIgnoreUnavailableNotMatching() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "_origin:logs",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of(),
                        ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE,
                        null
                    ),
                    Set.of()
                )
            )
        );

        expectThrows(
            IndexNotFoundException.class,
            containsString("no such index [_origin:logs]"),
            () -> crossProjectErrorsUtil.crossProjectFanoutErrorHandling(getStrictIgnoreUnavailable(), local, null)
        );
    }

    public void testQualifiedExpressionWithStrictIgnoreUnavailableMatchingInLinkedProject() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "P1:logs",
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
        crossProjectErrorsUtil.crossProjectFanoutErrorHandling(getStrictIgnoreUnavailable(), local, remote);
    }

    public void testMissingQualifiedExpressionWithStrictIgnoreUnavailable() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "P1:logs",
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

        expectThrows(
            IndexNotFoundException.class,
            containsString("no such index [P1:logs]"),
            () -> crossProjectErrorsUtil.crossProjectFanoutErrorHandling(getStrictIgnoreUnavailable(), local, remote)
        );
    }

    public void testUnauthorizedQualifiedExpressionWithStrictIgnoreUnavailable() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "P1:logs",
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
                            ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED,
                            new ElasticsearchException("logs")
                        ),
                        Set.of()
                    )
                )
            )
        );

        expectThrows(
            ElasticsearchSecurityException.class,
            containsString("authorization errors while resolving [P1:logs]"),
            () -> crossProjectErrorsUtil.crossProjectFanoutErrorHandling(getStrictIgnoreUnavailable(), local, remote)
        );
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
        crossProjectErrorsUtil.crossProjectFanoutErrorHandling(getStrictAllowNoIndices(), local, null);
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
        crossProjectErrorsUtil.crossProjectFanoutErrorHandling(getStrictAllowNoIndices(), local, remote);
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

        expectThrows(
            IndexNotFoundException.class,
            containsString("no such index [P1:logs*]"),
            () -> crossProjectErrorsUtil.crossProjectFanoutErrorHandling(getStrictAllowNoIndices(), local, remote)
        );
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

        expectThrows(
            IndexNotFoundException.class,
            containsString("no such index [P1:logs*]"),
            () -> crossProjectErrorsUtil.crossProjectFanoutErrorHandling(getStrictAllowNoIndices(), local, remote)
        );
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
        crossProjectErrorsUtil.crossProjectFanoutErrorHandling(getStrictAllowNoIndices(), local, null);
    }

    public void testQualifiedOriginExpressionWithStrictAllowNoIndicesNotMatching() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "_origin:logs*",
                    new ResolvedIndexExpression.LocalExpressions(
                        Set.of(),
                        ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS,
                        null
                    ),
                    Set.of()
                )
            )
        );

        expectThrows(
            IndexNotFoundException.class,
            containsString("no such index [_origin:logs*]"),
            () -> crossProjectErrorsUtil.crossProjectFanoutErrorHandling(getStrictAllowNoIndices(), local, null)
        );
    }

    public void testQualifiedExpressionWithStrictAllowNoIndicesMatchingInLinkedProject() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "P1:logs*",
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
        crossProjectErrorsUtil.crossProjectFanoutErrorHandling(getStrictAllowNoIndices(), local, remote);
    }

    public void testMissingQualifiedExpressionWithStrictAllowNoIndices() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "P1:logs*",
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

        expectThrows(
            IndexNotFoundException.class,
            containsString("no such index [P1:logs*]"),
            () -> crossProjectErrorsUtil.crossProjectFanoutErrorHandling(getStrictAllowNoIndices(), local, remote)
        );
    }

    public void testUnauthorizedQualifiedExpressionWithStrictAllowNoIndices() {
        ResolvedIndexExpressions local = new ResolvedIndexExpressions(
            List.of(
                new ResolvedIndexExpression(
                    "P1:logs*",
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

        expectThrows(
            IndexNotFoundException.class,
            containsString("no such index [P1:logs*]"),
            () -> crossProjectErrorsUtil.crossProjectFanoutErrorHandling(getStrictAllowNoIndices(), local, remote)
        );
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
