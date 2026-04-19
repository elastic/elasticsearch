/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.esql.datasource.DeleteDataSourceAction.Request;

import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Round-trip wire test for {@link Request} ({@code AcknowledgedRequest} subclass, transported
 * to the master).
 */
public class DeleteDataSourceActionRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        return new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, randomAlphaOfLengthBetween(1, 20).toLowerCase(Locale.ROOT));
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request mutateInstance(Request instance) {
        return new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, instance.name() + "_mutated");
    }

    // -- validate() ------------------------------------------------------------------------------

    public void testValidateAcceptsCleanRequest() {
        assertThat(new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "my_ds").validate(), nullValue());
    }

    public void testValidateRejectsEmptyName() {
        ActionRequestValidationException v = new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "").validate();
        assertThat(v, notNullValue());
        assertThat(v.getMessage(), containsString("data source name is missing"));
    }
}
