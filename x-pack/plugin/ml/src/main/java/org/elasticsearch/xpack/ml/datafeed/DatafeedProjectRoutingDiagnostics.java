/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.crossproject.NoMatchingProjectException;

import java.text.MessageFormat;
import java.util.Locale;

/**
 * Actionable user-facing messages when {@code project_routing} fails to resolve linked projects.
 *
 * <p>These templates stay local rather than moving to {@code Messages.java}: each interpolates three
 * dynamic pieces (datafeed id, routing expression, upstream cause message) into prose long enough that
 * folding it into {@code Messages.java}'s flat {@code {0}}/{@code {1}} placeholder list would hurt
 * readability more than centralizing helps.
 */
public final class DatafeedProjectRoutingDiagnostics {

    public enum Phase {
        VALIDATE_BEFORE_MINT,
        RUNTIME
    }

    private static final String VALIDATE_BEFORE_MINT_TEMPLATE = "Cannot update datafeed [{0}]: project_routing [{1}] matched no "
        + "linked project ({2}). Link the missing project in Elastic Cloud project settings, or update project_routing to a valid "
        + "linked alias (for example _origin for local-only scope).";

    private static final String RUNTIME_TEMPLATE = "Datafeed [{0}] cannot search any project: project_routing [{1}] matched no "
        + "linked projects at run time ({2}). Link the missing project(s) in Elastic Cloud project settings, or update "
        + "project_routing to an expression that matches at least one linked project (for example _origin for local-only scope).";

    private DatafeedProjectRoutingDiagnostics() {}

    public static Exception enrichIfNoMatchingProject(String datafeedId, @Nullable String projectRouting, Exception failure) {
        return enrichIfNoMatchingProject(datafeedId, projectRouting, failure, Phase.RUNTIME);
    }

    public static Exception enrichIfNoMatchingProject(String datafeedId, @Nullable String projectRouting, Exception failure, Phase phase) {
        Throwable unwrapped = ExceptionsHelper.unwrap(failure, NoMatchingProjectException.class);
        if (unwrapped instanceof NoMatchingProjectException noMatchingProject) {
            NoMatchingProjectException enriched = new NoMatchingProjectException(
                formatMessage(datafeedId, projectRouting, noMatchingProject, phase)
            );
            enriched.initCause(noMatchingProject);
            return enriched;
        }
        return failure;
    }

    private static String formatMessage(String datafeedId, @Nullable String projectRouting, NoMatchingProjectException cause, Phase phase) {
        String routing = projectRouting != null ? projectRouting : "unspecified";
        String causeMessage = cause.getMessage();
        String template = phase == Phase.VALIDATE_BEFORE_MINT ? VALIDATE_BEFORE_MINT_TEMPLATE : RUNTIME_TEMPLATE;
        return new MessageFormat(template, Locale.ROOT).format(new Object[] { datafeedId, routing, causeMessage });
    }
}
