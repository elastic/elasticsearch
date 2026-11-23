/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.is;

public class RestClusterRerouteActionTests extends ESTestCase {

    public void testCanNotTripCircuitBreaker() {
        assertThat(
            new RestClusterRerouteAction(new SettingsFilter(List.of()), TestProjectResolvers.DEFAULT_PROJECT_ONLY).canTripCircuitBreaker(),
            is(false)
        );
    }
}
