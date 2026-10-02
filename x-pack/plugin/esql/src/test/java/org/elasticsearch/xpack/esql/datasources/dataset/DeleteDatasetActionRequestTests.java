/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.dataset;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.esql.datasources.dataset.DeleteDatasetAction.Request;

import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/** Wire round-trip + validate() tests for {@link Request}. */
public class DeleteDatasetActionRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        int count = between(1, 3);
        String[] names = new String[count];
        for (int i = 0; i < count; i++) {
            names[i] = randomAlphaOfLengthBetween(1, 20).toLowerCase(Locale.ROOT);
        }
        return new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, names);
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request mutateInstance(Request instance) {
        String[] names = instance.names().clone();
        names[0] = names[0] + "_mutated";
        return new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, names);
    }

    public void testValidateAcceptsCleanRequest() {
        assertThat(new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new String[] { "my_ds" }).validate(), nullValue());
    }

    public void testValidateRejectsEmptyNames() {
        ActionRequestValidationException v = new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new String[0]).validate();
        assertThat(v, notNullValue());
        assertThat(v.getMessage(), containsString("dataset names cannot be empty"));
    }
}
