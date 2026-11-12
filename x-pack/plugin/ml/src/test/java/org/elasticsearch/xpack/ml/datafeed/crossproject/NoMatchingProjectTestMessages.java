/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.crossproject;

import org.elasticsearch.search.crossproject.CrossProjectIndexExpressionsRewriter;
import org.elasticsearch.search.crossproject.NoMatchingProjectException;

import java.util.Set;

/**
 * Builds {@link NoMatchingProjectException} instances with the same message text as index resolution in production.
 */
public final class NoMatchingProjectTestMessages {

    private NoMatchingProjectTestMessages() {}

    public static NoMatchingProjectException noProjectAfterRouting(String projectRouting) {
        try {
            CrossProjectIndexExpressionsRewriter.rewriteIndexExpression("logs", null, Set.of(), projectRouting);
            throw new AssertionError("expected NoMatchingProjectException for routing [" + projectRouting + "]");
        } catch (NoMatchingProjectException e) {
            return e;
        }
    }

    public static NoMatchingProjectException explicitMissingQualifiedProject() {
        try {
            CrossProjectIndexExpressionsRewriter.validateIndexExpressionWithoutRewrite(
                "missing-alias:logs-*",
                "origin-alias",
                Set.of("origin-alias", "linked-one"),
                "_alias:*"
            );
            throw new AssertionError("expected NoMatchingProjectException for missing qualified project alias");
        } catch (NoMatchingProjectException e) {
            return e;
        }
    }
}
