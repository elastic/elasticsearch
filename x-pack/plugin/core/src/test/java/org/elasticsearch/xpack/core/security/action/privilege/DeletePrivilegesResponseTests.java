/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;

public class DeletePrivilegesResponseTests extends ESTestCase {

    public void testSerialization() throws IOException {
        final DeletePrivilegesResponse original = new DeletePrivilegesResponse(
            Arrays.asList(generateRandomStringArray(5, randomIntBetween(3, 8), false, true)));

        final BytesStreamOutput output = new BytesStreamOutput();
        original.writeTo(output);
        output.flush();
        final DeletePrivilegesResponse copy = new DeletePrivilegesResponse(output.bytes().streamInput());
        assertThat(copy.found(), equalTo(original.found()));
    }
}
