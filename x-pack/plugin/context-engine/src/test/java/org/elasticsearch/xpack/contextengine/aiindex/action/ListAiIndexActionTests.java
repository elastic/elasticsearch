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
import org.elasticsearch.xpack.contextengine.aiindex.AiIndexTestUtils;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ListAiIndexActionTests extends AbstractWireSerializingTestCase<ListAiIndexAction.Response> {

    @Override
    protected Writeable.Reader<ListAiIndexAction.Response> instanceReader() {
        return ListAiIndexAction.Response::new;
    }

    @Override
    protected ListAiIndexAction.Response createTestInstance() {
        return new ListAiIndexAction.Response(randomList(0, 5, AiIndexTestUtils::randomAiIndex));
    }

    @Override
    protected ListAiIndexAction.Response mutateInstance(ListAiIndexAction.Response instance) {
        return new ListAiIndexAction.Response(
            randomValueOtherThan(instance.aiIndices(), () -> randomList(0, 5, AiIndexTestUtils::randomAiIndex))
        );
    }

    public void testRequestSerializationAndValidation() throws Exception {
        ListAiIndexAction.Request request = new ListAiIndexAction.Request(
            randomBoolean() ? null : randomAlphaOfLength(8),
            randomBoolean() ? null : randomAlphaOfLength(8),
            randomIntBetween(1, 200)
        );
        assertEquals(request, copyWriteable(request, getNamedWriteableRegistry(), ListAiIndexAction.Request::new));
        assertThat(request.validate(), nullValue());

        ActionRequestValidationException e = new ListAiIndexAction.Request(null, null, 0).validate();
        assertThat(e, notNullValue());
        assertThat(e.getMessage(), containsString("[size] must be greater than 0"));
    }
}
