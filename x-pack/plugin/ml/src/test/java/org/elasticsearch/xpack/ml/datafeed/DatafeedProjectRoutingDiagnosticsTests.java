/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.search.crossproject.NoMatchingProjectException;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

public class DatafeedProjectRoutingDiagnosticsTests extends ESTestCase {

    public void testEnrichWhenCauseIsNotNoMatchingProjectShouldReturnOriginalFailure() {
        RuntimeException failure = new RuntimeException("connection reset");
        assertThat(DatafeedProjectRoutingDiagnostics.enrichIfNoMatchingProject("df-1", "_alias:prod-*", failure), sameInstance(failure));
    }

    public void testEnrichRuntimeWhenRoutingMatchesNoProjectShouldIncludeDatafeedAndRouting() {
        NoMatchingProjectException cause = new NoMatchingProjectException(
            "no matching project after applying project routing [_alias:missing-*]"
        );
        Exception enriched = DatafeedProjectRoutingDiagnostics.enrichIfNoMatchingProject(
            "my-datafeed",
            "_alias:missing-*",
            cause,
            DatafeedProjectRoutingDiagnostics.Phase.RUNTIME
        );
        assertThat(enriched, instanceOf(NoMatchingProjectException.class));
        assertThat(enriched.getMessage(), containsString("my-datafeed"));
        assertThat(enriched.getMessage(), containsString("_alias:missing-*"));
        assertThat(enriched.getMessage(), containsString("matched no linked project"));
        assertThat(enriched.getMessage(), containsString("project_routing"));
        assertThat(enriched.getMessage(), containsString("_origin"));
        assertThat(enriched.getCause(), equalTo(cause));
    }

    public void testEnrichValidateBeforeMintWhenQualifiedProjectMissingShouldIncludeAlias() {
        NoMatchingProjectException cause = new NoMatchingProjectException("nonexistent_project", "_alias:*");
        Exception enriched = DatafeedProjectRoutingDiagnostics.enrichIfNoMatchingProject(
            "my-datafeed",
            "_alias:*",
            cause,
            DatafeedProjectRoutingDiagnostics.Phase.VALIDATE_BEFORE_MINT
        );
        assertThat(enriched.getMessage(), containsString("Cannot update datafeed [my-datafeed]"));
        assertThat(enriched.getMessage(), containsString("nonexistent_project"));
        assertThat(enriched.getMessage(), containsString("_alias:*"));
        assertThat(enriched.getMessage(), containsString("Link the missing project"));
        assertThat(enriched.getCause(), equalTo(cause));
    }
}
