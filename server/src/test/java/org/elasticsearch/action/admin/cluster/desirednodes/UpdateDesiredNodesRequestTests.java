/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.desirednodes;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class UpdateDesiredNodesRequestTests extends ESTestCase {
    public void testValidation() {
        final UpdateDesiredNodesRequest updateDesiredNodesRequest = new UpdateDesiredNodesRequest(
            randomBoolean() ? "" : "     ",
            -1,
            Collections.emptyList()
        );
        ActionRequestValidationException exception = updateDesiredNodesRequest.validate();
        assertThat(exception, is(notNullValue()));
        assertThat(exception.getMessage(), containsString("version must be positive"));
        assertThat(exception.getMessage(), containsString("nodes must not contain at least one node"));
        assertThat(exception.getMessage(), containsString("historyID should not be empty"));
    }
}
