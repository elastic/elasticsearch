/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.project;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.test.ESIntegTestCase;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;

public class DefaultProjectResolverIT extends ESIntegTestCase {

    public void testDefaultProjectResolver() {
        internalCluster().getInstances(ProjectResolver.class).forEach(projectResolver -> {
            assertThat(projectResolver, isA(DefaultProjectResolver.class));
            assertThat(projectResolver.getProjectId(), is(Metadata.DEFAULT_PROJECT_ID));
            assertThat(projectResolver.supportsMultipleProjects(), is(false));
        });
    }
}
