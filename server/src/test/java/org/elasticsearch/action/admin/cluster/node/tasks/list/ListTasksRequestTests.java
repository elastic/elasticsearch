/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.tasks.list;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class ListTasksRequestTests extends ESTestCase {

    public void testValidation() {
        ActionRequestValidationException ex = new ListTasksRequest().setDescriptions("foo*").validate();
        assertThat(ex, is(not(nullValue())));

        ex = new ListTasksRequest().setDescriptions("foo*").setDetailed(true).validate();
        assertThat(ex, is(nullValue()));
    }

    public void testMatch() {
        ListTasksRequest filterOnDescription = new ListTasksRequest().setDescriptions("foo*", "*bar", "absolute").setActions("my_action*");

        assertTrue(filterOnDescription.match(taskWithActionDescription("my_action_foo", "foo_action")));
        assertTrue(filterOnDescription.match(taskWithActionDescription("my_action_bar", "absolute")));
        assertTrue(filterOnDescription.match(taskWithActionDescription("my_action_baz", "action_bar")));

        assertFalse(filterOnDescription.match(taskWithActionDescription("my_action_foo", "not_wanted")));
        assertFalse(filterOnDescription.match(taskWithActionDescription("not_wanted_action", "foo_action")));


        ListTasksRequest notFilterOnDescription = new ListTasksRequest().setActions("my_action*");
        assertTrue(notFilterOnDescription.match(taskWithActionDescription("my_action_foo", "foo_action")));
        assertTrue(notFilterOnDescription.match(taskWithActionDescription("my_action_bar", "absolute")));
        assertTrue(notFilterOnDescription.match(taskWithActionDescription("my_action_baz", "action_bar")));
        assertTrue(notFilterOnDescription.match(taskWithActionDescription("my_action_baz", randomAlphaOfLength(10))));

        assertFalse(notFilterOnDescription.match(taskWithActionDescription("not_wanted_action", randomAlphaOfLength(10))));
    }

    private static Task taskWithActionDescription(String action, String description) {
        return new Task(
            randomNonNegativeLong(),
            randomAlphaOfLength(10),
            action,
            description,
            new TaskId("test_node", randomNonNegativeLong()),
            Collections.emptyMap()
        );
    }

}
