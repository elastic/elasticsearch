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
        final var cpsIndicesOptions = IndicesOptions.builder(IndicesOptions.DEFAULT)
            .crossProjectModeOptions(new IndicesOptions.CrossProjectModeOptions(true))
            .build();

        final var notAllowed = new IndicesRequestImpl(false, randomFrom(IndicesOptions.DEFAULT, cpsIndicesOptions));
        final var noCpsOption = new IndicesRequestImpl(true, IndicesOptions.DEFAULT);
        final var withCpsOption = new IndicesRequestImpl(true, cpsIndicesOptions);

        assertFalse(crossProjectModeDecider.resolvesCrossProject(notAllowed));
        assertFalse(crossProjectModeDecider.resolvesCrossProject(noCpsOption));

        assertThat(crossProjectModeDecider.resolvesCrossProject(withCpsOption), is(expected));
    }

    private static class IndicesRequestImpl implements IndicesRequest, IndicesRequest.CrossProjectCandidate {

        private final boolean allowsCrossProject;
        private final IndicesOptions indicesOptions;

        IndicesRequestImpl(boolean allowsCrossProject, IndicesOptions indicesOptions) {
            this.allowsCrossProject = allowsCrossProject;
            this.indicesOptions = indicesOptions;
        }

        @Override
        public boolean allowsCrossProject() {
            return allowsCrossProject;
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
