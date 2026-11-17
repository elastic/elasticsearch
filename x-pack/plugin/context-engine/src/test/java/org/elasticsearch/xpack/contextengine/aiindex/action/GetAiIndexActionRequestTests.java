/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.aiindex.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class GetAiIndexActionRequestTests extends AbstractWireSerializingTestCase<GetAiIndexAction.Request> {

    @Override
    protected Writeable.Reader<GetAiIndexAction.Request> instanceReader() {
        return GetAiIndexAction.Request::new;
    }

    @Override
    protected GetAiIndexAction.Request createTestInstance() {
        return new GetAiIndexAction.Request(randomAlphaOfLength(8), randomAlphaOfLength(10));
    }

    @Override
    protected GetAiIndexAction.Request mutateInstance(GetAiIndexAction.Request instance) {
        return new GetAiIndexAction.Request(instance.space(), randomValueOtherThan(instance.id(), () -> randomAlphaOfLength(10)));
    }

    public void testValidate() {
        assertThat(createTestInstance().validate(), nullValue());

        ActionRequestValidationException e = new GetAiIndexAction.Request("", "").validate();
        assertThat(e, notNullValue());
        assertThat(e.getMessage(), containsString("[space] cannot be null or empty"));
        assertThat(e.getMessage(), containsString("[id] cannot be null or empty"));

        e = new DeleteAiIndexAction.Request(null, "x").validate();
        assertThat(e, notNullValue());
        assertThat(e.getMessage(), containsString("[space] cannot be null or empty"));
    }
}
