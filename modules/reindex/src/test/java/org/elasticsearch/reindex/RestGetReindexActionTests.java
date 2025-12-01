/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.junit.Before;

import static org.hamcrest.Matchers.equalTo;

public class RestGetReindexActionTests extends RestActionTestCase {

    private RestGetReindexAction action;

    @Before
    public void setUpAction() {
        action = new RestGetReindexAction();
        controller().registerHandler(action);
    }

    public void testRoute() {
        assertThat(action.routes().size(), equalTo(1));
        assertThat(action.routes().get(0).getPath(), equalTo("/_reindex/{task_id}"));
        assertThat(action.routes().get(0).getMethod(), equalTo(RestRequest.Method.GET));
    }

    public void testName() {
        assertThat(action.getName(), equalTo("get_reindex_action"));
    }
}
