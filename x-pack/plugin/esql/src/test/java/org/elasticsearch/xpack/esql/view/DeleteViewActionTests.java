/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class DeleteViewActionTests extends ESTestCase {

    public void testValidateWithMultipleViews() {
        DeleteViewAction.Request request = new DeleteViewAction.Request(
            TimeValue.ONE_MINUTE,
            TimeValue.ONE_MINUTE,
            new String[] { "view1", "view2", "view3" }
        );
        assertThat(request.validate(), nullValue());
    }

    public void testValidateEmptyArray() {
        DeleteViewAction.Request request = new DeleteViewAction.Request(TimeValue.ONE_MINUTE, TimeValue.ONE_MINUTE, new String[0]);
        ActionRequestValidationException validation = request.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.getMessage(), containsString("views cannot be null or missing"));
    }

    public void testIndicesReturnsSameAsViews() {
        String[] views = new String[] { "a", "b", "c" };
        DeleteViewAction.Request request = new DeleteViewAction.Request(TimeValue.ONE_MINUTE, TimeValue.ONE_MINUTE, views);
        assertArrayEquals(views, request.indices());
        assertSame(request.views(), request.indices());
    }
}
