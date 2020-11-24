/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.example;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.example.actions.GetActionsAction;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

// This test class is really to pass the testingConventions test
public class OpTestPluginTests extends ESTestCase {

    public void testActionWillBeProvided() {
        final OpTestPlugin opTestPlugin = new OpTestPlugin();
        final List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> actions = opTestPlugin.getActions();
        assertEquals(1, actions.size());
        assertSame(GetActionsAction.INSTANCE, actions.get(0).getAction());
    }

}
