/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.crud.dataset;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.esql.datasources.crud.dataset.DeleteDatasetAction.Request;

import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/** Wire round-trip + validate() tests for {@link Request}. */
public class DeleteDatasetActionRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        Request r = new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, randomAlphaOfLengthBetween(1, 20).toLowerCase(Locale.ROOT));
        // Request implements IndicesRequest.Replaceable. indices() is set from the name on
        // the HTTP/serialization boundary; pre-seed it here so equals() is stable after round-trip.
        r.indices(r.name());
        return r;
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request mutateInstance(Request instance) {
        Request mutated = new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, instance.name() + "_mutated");
        mutated.indices(mutated.name());
        return mutated;
    }

    // -- validate() ------------------------------------------------------------------------------

    public void testValidateAcceptsCleanRequest() {
        assertThat(new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "my_ds").validate(), nullValue());
    }

    public void testValidateRejectsEmptyName() {
        ActionRequestValidationException v = new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "").validate();
        assertThat(v, notNullValue());
        assertThat(v.getMessage(), containsString("dataset name is missing"));
    }
}
