/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.memory.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class SearchSessionMemoryActionRequestTests extends AbstractWireSerializingTestCase<SearchSessionMemoryAction.Request> {

    @Override
    protected Writeable.Reader<SearchSessionMemoryAction.Request> instanceReader() {
        return SearchSessionMemoryAction.Request::new;
    }

    @Override
    protected SearchSessionMemoryAction.Request createTestInstance() {
        return new SearchSessionMemoryAction.Request(
            randomQuery(),
            randomBoolean() ? null : randomAlphaOfLength(12),
            randomBoolean() ? null : randomAlphaOfLength(8),
            randomBoolean() ? null : randomAlphaOfLength(8),
            randomBoolean() ? null : randomList(1, 3, () -> randomAlphaOfLength(6)),
            randomIntBetween(1, 100)
        );
    }

    @Override
    protected SearchSessionMemoryAction.Request mutateInstance(SearchSessionMemoryAction.Request instance) {
        return new SearchSessionMemoryAction.Request(
            randomValueOtherThan(instance.query(), SearchSessionMemoryActionRequestTests::randomQuery),
            instance.sessionId(),
            instance.userId(),
            instance.space(),
            instance.aiIndexIds(),
            instance.size()
        );
    }

    private static Object randomQuery() {
        return randomBoolean()
            ? randomAlphaOfLengthBetween(1, 30)
            : Map.of("type", "image", "value", "data:image/png;base64," + randomAlphaOfLength(16));
    }

    public void testValidate() {
        assertThat(createTestInstance().validate(), nullValue());

        ActionRequestValidationException e = new SearchSessionMemoryAction.Request("", null, null, null, null, 0).validate();
        assertThat(e, notNullValue());
        assertThat(e.getMessage(), containsString("[query] cannot be empty"));
        assertThat(e.getMessage(), containsString("[size] must be between 1 and 100"));

        e = new SearchSessionMemoryAction.Request("dark mode", null, null, null, null, 101).validate();
        assertThat(e.getMessage(), containsString("[size] must be between 1 and 100"));

        e = new SearchSessionMemoryAction.Request(Map.of("type", "image"), null, null, null, null, 10).validate();
        assertThat(e, notNullValue());
        assertThat(e.getMessage(), containsString("[query] objects must have a non-empty [type] and [value]"));

        e = new SearchSessionMemoryAction.Request(42, null, null, null, null, 10).validate();
        assertThat(e, notNullValue());
        assertThat(e.getMessage(), containsString("[query] must be a string or an object"));

        e = new SearchSessionMemoryAction.Request(Map.of("type", "gif", "value", "abc", "format", "hex"), null, null, null, null, 10)
            .validate();
        assertThat(e, notNullValue());
        assertThat(e.getMessage(), containsString("[query] Unrecognized type [gif]"));
        assertThat(e.getMessage(), containsString("[query] Unrecognized format [hex]"));

        e = new SearchSessionMemoryAction.Request(Map.of("type", "image", "value", "abc", "format", 1), null, null, null, null, 10)
            .validate();
        assertThat(e, notNullValue());
        assertThat(e.getMessage(), containsString("[query] [format] must be a string"));
    }
}
