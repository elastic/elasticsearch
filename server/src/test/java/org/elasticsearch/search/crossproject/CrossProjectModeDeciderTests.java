/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
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
            new CrossProjectCandidateImpl(false),
            new IndicesRequestImpl(false, randomFrom(IndicesOptions.DEFAULT, cpsIndicesOptions))
        );
        final var candidateAndAllowed = new CrossProjectCandidateImpl(true);

        final var indicesRequestNoCpsOption = new IndicesRequestImpl(true, IndicesOptions.DEFAULT);
        final var indicesRequestWithCpsOption = new IndicesRequestImpl(true, cpsIndicesOptions);

        assertFalse(crossProjectModeDecider.resolvesCrossProject(candidateButNotAllowed));
        assertFalse(crossProjectModeDecider.resolvesCrossProject(indicesRequestNoCpsOption));

        assertThat(crossProjectModeDecider.resolvesCrossProject(candidateAndAllowed), is(expected));
        assertThat(crossProjectModeDecider.resolvesCrossProject(indicesRequestWithCpsOption), is(expected));
    }

    private static class CrossProjectCandidateImpl implements IndicesRequest.CrossProjectCandidate {

        private boolean allowsCrossProject;

        CrossProjectCandidateImpl(boolean allowsCrossProject) {
            this.allowsCrossProject = allowsCrossProject;
        }

        @Override
        public boolean allowsCrossProject() {
            return allowsCrossProject;
        }
    }

    private static class IndicesRequestImpl extends CrossProjectCandidateImpl implements IndicesRequest {

        private IndicesOptions indicesOptions;

        IndicesRequestImpl(boolean allowsCrossProject, IndicesOptions indicesOptions) {
            super(allowsCrossProject);
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
