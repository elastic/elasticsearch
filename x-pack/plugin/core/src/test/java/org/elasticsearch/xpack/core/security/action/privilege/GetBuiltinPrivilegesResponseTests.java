/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;

public class GetBuiltinPrivilegesResponseTests extends ESTestCase {

    public void testSerialization() throws IOException {
        final String[] cluster = generateRandomStringArray(8, randomIntBetween(3, 8), false, true);
        final String[] index = generateRandomStringArray(8, randomIntBetween(3, 8), false, true);
        final GetBuiltinPrivilegesResponse original = new GetBuiltinPrivilegesResponse(cluster, index);

        final BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        final GetBuiltinPrivilegesResponse copy = new GetBuiltinPrivilegesResponse(out.bytes().streamInput());

        assertThat(copy.getClusterPrivileges(), Matchers.equalTo(cluster));
        assertThat(copy.getIndexPrivileges(), Matchers.equalTo(index));
    }

}
