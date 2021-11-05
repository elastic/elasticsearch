/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ccr.action.ActivateAutoFollowPatternAction;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ActivateAutoFollowPatternActionRequestTests extends AbstractWireSerializingTestCase<ActivateAutoFollowPatternAction.Request> {

    @Override
    protected ActivateAutoFollowPatternAction.Request createTestInstance() {
        return new ActivateAutoFollowPatternAction.Request(randomAlphaOfLength(5), randomBoolean());
    }

    @Override
    protected Writeable.Reader<ActivateAutoFollowPatternAction.Request> instanceReader() {
        return ActivateAutoFollowPatternAction.Request::new;
    }

    public void testValidate() {
        ActivateAutoFollowPatternAction.Request request = new ActivateAutoFollowPatternAction.Request(null, true);
        ActionRequestValidationException validationException = request.validate();
        assertThat(validationException, notNullValue());
        assertThat(validationException.getMessage(), containsString("[name] is missing"));

        request = new ActivateAutoFollowPatternAction.Request("name", true);
        validationException = request.validate();
        assertThat(validationException, nullValue());
    }
}
