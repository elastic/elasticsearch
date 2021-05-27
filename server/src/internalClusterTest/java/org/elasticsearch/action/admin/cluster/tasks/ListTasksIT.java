/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.tasks;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.test.ESIntegTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class ListTasksIT extends ESIntegTestCase {

    public void testListTasksFilteredByDescription() {

        // The list tasks action itself is filtered out via this description filter
        assertThat(
            client().admin().cluster().prepareListTasks().setDetailed(true).setDescriptions("match_nothing*").get().getTasks(),
            is(empty())
        );

        // The list tasks action itself is kept via this description filter which matches everything
        assertThat(
            client().admin().cluster().prepareListTasks().setDetailed(true).setDescriptions("*").get().getTasks(),
            is(not(empty()))
        );

    }

    public void testListTasksValidation() {

        ActionRequestValidationException ex = expectThrows(
            ActionRequestValidationException.class,
            () -> client().admin().cluster().prepareListTasks().setDescriptions("*").get()
        );
        assertThat(ex.getMessage(), containsString("matching on descriptions is not available when [detailed] is false"));

    }
}
