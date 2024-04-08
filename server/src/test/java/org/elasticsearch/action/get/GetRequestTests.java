/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.get;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;

public class GetRequestTests extends ESTestCase {

    public void testValidation() {
        {
            final GetRequest request = new GetRequest("index4", "0");
            final ActionRequestValidationException validate = request.validate();

            assertThat(validate, nullValue());
        }

        {
            final GetRequest request = new GetRequest("index4", randomBoolean() ? "" : null);
            final ActionRequestValidationException validate = request.validate();

            assertThat(validate, not(nullValue()));
            assertEquals(1, validate.validationErrors().size());
            assertThat(validate.validationErrors(), hasItems("id is missing"));
        }
    }

    public void testForceSyntheticUnsupported() {
        GetRequest request = new GetRequest("index", "id");
        request.setForceSyntheticSource(true);
        StreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(TransportVersions.V_8_3_0);
        Exception e = expectThrows(IllegalArgumentException.class, () -> request.writeTo(out));
        assertEquals(e.getMessage(), "force_synthetic_source is not supported before 8.4.0");
    }
}
