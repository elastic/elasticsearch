/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.crossproject;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

public class CrossProjectModeDeciderTests extends ESTestCase {

    public void testResolvesCrossProject() {
        doTestResolvesCrossProject(new CrossProjectModeDecider(Settings.builder().build()), false);
        doTestResolvesCrossProject(
            new CrossProjectModeDecider(Settings.builder().put("serverless.cross_project.enabled", true).build()),
            true
        );
    }

    private void doTestResolvesCrossProject(CrossProjectModeDecider crossProjectModeDecider, boolean expected) {
        final var cpsIndicesOptions = IndicesOptions.builder(org.elasticsearch.action.support.IndicesOptions.DEFAULT)
            .crossProjectModeOptions(new IndicesOptions.CrossProjectModeOptions(true))
            .build();

        final var candidateButNotAllowed = randomFrom(
            new CrossProjectCandidateImpl(false, false),
            new CrossProjectCandidateImpl(false, true),
            new IndicesRequestImpl(false, randomFrom(IndicesOptions.DEFAULT, cpsIndicesOptions))
        );
        final var candidateAllowedButNotResolving = new CrossProjectCandidateImpl(true, false);
        final var candidateAndResolving = new CrossProjectCandidateImpl(true, true);

        final var indicesRequestNoCpsOption = new IndicesRequestImpl(true, IndicesOptions.DEFAULT);
        final var indicesRequestWithCpsOption = new IndicesRequestImpl(true, cpsIndicesOptions);

        assertFalse(crossProjectModeDecider.resolvesCrossProject(candidateButNotAllowed));
        assertFalse(crossProjectModeDecider.resolvesCrossProject(candidateAllowedButNotResolving));
        assertFalse(crossProjectModeDecider.resolvesCrossProject(indicesRequestNoCpsOption));

        assertThat(crossProjectModeDecider.resolvesCrossProject(candidateAndResolving), is(expected));
        assertThat(crossProjectModeDecider.resolvesCrossProject(indicesRequestWithCpsOption), is(expected));
    }

    private static class CrossProjectCandidateImpl implements IndicesRequest.CrossProjectCandidate {

        private final boolean allowsCrossProject;
        private final boolean resolvesCrossProject;

        CrossProjectCandidateImpl(boolean allowsCrossProject, boolean resolvesCrossProject) {
            this.allowsCrossProject = allowsCrossProject;
            this.resolvesCrossProject = resolvesCrossProject;
        }

        @Override
        public boolean allowsCrossProject() {
            return allowsCrossProject;
        }

        @Override
        public boolean resolvesCrossProject() {
            return resolvesCrossProject;
        }
    }

    private static class IndicesRequestImpl extends CrossProjectCandidateImpl implements IndicesRequest {

        private final IndicesOptions indicesOptions;

        IndicesRequestImpl(boolean allowsCrossProject, IndicesOptions indicesOptions) {
            super(allowsCrossProject, indicesOptions.resolveCrossProjectIndexExpression());
            this.indicesOptions = indicesOptions;
        }

        @Override
        public String[] indices() {
            return new String[0];
        }

        @Override
        public IndicesOptions indicesOptions() {
            return indicesOptions;
        }
    }
}
